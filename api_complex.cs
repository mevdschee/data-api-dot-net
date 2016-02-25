using System;
using System.Web;
using System.Data;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Web.Script.Serialization;
using System.Text.RegularExpressions;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;

namespace DataApiDotNet_Complex
{
	delegate bool TableAuthorizerDelegate  (string action, string database, string table);
	delegate bool ColumnAuthorizerDelegate (string action, string database, string table, string column);
	delegate bool InputSanitizerDelegate   (string action, string database, string table, string column, string type, object value);
	delegate bool InputValidatorDelegate   (string action, string database, string table, string column, string type, object value, NameValueCollection context);

	class Config
	{
		public string Username;
		public string Password;
		public string Database;
		// for connectivity (defaults to localhost):
		public string Hostname;
		public string Port;
		public string Socket;
		public string Charset;
		// callbacks with their default behavior
		public TableAuthorizerDelegate TableAuthorizer;
		public ColumnAuthorizerDelegate ColumnAuthorizer;
		public InputSanitizerDelegate InputSanitizer;
		public InputValidatorDelegate InputValidator;
		// dependencies (added for unit testing):
		public IDbConnection Db;
		public string Method;
		public string Request;
		public NameValueCollection Get;
		public System.IO.Stream Post;
	}

	class MySQL_CRUD_API: REST_CRUD_API
	{

		// interfaces that the MySql libraries implement, namely IDbConnection, IDbCommand, IDataParameters,

		public MySQL_CRUD_API(HttpContext context, Config config): base(context,config)
		{
			_queries = new Dictionary<string,string>() {
				{ "reflect_table", "SELECT " +
						"\"TABLE_NAME\" " +
					"FROM " +
						"\"INFORMATION_SCHEMA\".\"TABLES\" " +
					"WHERE " +
						"\"TABLE_NAME\" COLLATE 'utf8_bin' = ? AND " +
						"\"TABLE_SCHEMA\" = ?" },
				{ "reflect_pk", "SELECT " +
						"\"COLUMN_NAME\" " +
					"FROM " +
						"\"INFORMATION_SCHEMA\".\"COLUMNS\" " +
					"WHERE " +
						"\"COLUMN_KEY\" = \'PRI\' AND " +
						"\"TABLE_NAME\" = ? AND " +
						"\"TABLE_SCHEMA\" = ?" },
				{ "reflect_belongs_to", "SELECT " +
						"\"TABLE_NAME\",\"COLUMN_NAME\", " +
						"\"REFERENCED_TABLE_NAME\",\"REFERENCED_COLUMN_NAME\" " +
					"FROM " +
						"\"INFORMATION_SCHEMA\".\"KEY_COLUMN_USAGE\" " +
					"WHERE " +
						"\"TABLE_NAME\" COLLATE \'utf8_bin\' = ? AND " +
						"\"REFERENCED_TABLE_NAME\" COLLATE \'utf8_bin\' IN ? AND " +
						"\"TABLE_SCHEMA\" = ? AND " +
						"\"REFERENCED_TABLE_SCHEMA\" = ?'," },
				{ "reflect_has_many", "SELECT " +
						"\"TABLE_NAME\",\"COLUMN_NAME\", " +
						"\"REFERENCED_TABLE_NAME\",\"REFERENCED_COLUMN_NAME\" " +
					"FROM " +
						"\"INFORMATION_SCHEMA\".\"KEY_COLUMN_USAGE\" " +
					"WHERE " +
						"\"TABLE_NAME\" COLLATE \'utf8_bin\' IN ? AND " +
						"\"REFERENCED_TABLE_NAME\" COLLATE \'utf8_bin\' = ? AND " +
						"\"TABLE_SCHEMA\" = ? AND " +
					"\"REFERENCED_TABLE_SCHEMA\" = ?'," },
				{ "reflect_habtm", "SELECT " +
						"k1.\"TABLE_NAME\", k1.\"COLUMN_NAME\", " +
						"k1.\"REFERENCED_TABLE_NAME\", k1.\"REFERENCED_COLUMN_NAME\", " +
						"k2.\"TABLE_NAME\", k2.\"COLUMN_NAME\", " +
						"k2.\"REFERENCED_TABLE_NAME\", k2.\"REFERENCED_COLUMN_NAME\" " +
					"FROM " +
						"\"INFORMATION_SCHEMA\".\"KEY_COLUMN_USAGE\" k1, " +
						"\"INFORMATION_SCHEMA\".\"KEY_COLUMN_USAGE\" k2 " +
					"WHERE " +
						"k1.\"TABLE_SCHEMA\" = ? AND " +
						"k2.\"TABLE_SCHEMA\" = ? AND " +
						"k1.\"REFERENCED_TABLE_SCHEMA\" = ? AND " +
						"k2.\"REFERENCED_TABLE_SCHEMA\" = ? AND " +
						"k1.\"TABLE_NAME\" COLLATE \'utf8_bin\' = k2.\"TABLE_NAME\" COLLATE \'utf8_bin\' AND " +
						"k1.\"REFERENCED_TABLE_NAME\" COLLATE \'utf8_bin\' = ? AND " +
						"k2.\"REFERENCED_TABLE_NAME\" COLLATE \'utf8_bin\' IN ?'" }
			};
		}

		override protected IDbConnection ConnectDatabase(string hostname,string username,string password,string database,string port,string socket,string charset)
		{
			MySqlConnection db=null;
			string connectionString =
				"Server=" + hostname + ";" +
				"Database=" + database + ";" +
				"User ID=" + username + ";" +
				"Password=" + password + ";" +
				"Pooling=true";
			try {
				db = new MySqlConnection(connectionString);
				db.Open();
			} catch (MySqlException ex) {
				throw new Exception("Connect failed. "+ex.Message);
			}
			if (socket!=null) {
				throw new Exception("Socket connection is not supported.");
			}
			try {
				(new MySqlCommand("SET SESSION sql_mode = 'ANSI_QUOTES';", db)).ExecuteNonQuery ();
			} catch (MySqlException ex) {
				throw new Exception("Error setting ANSI quotes. " + ex.Message);
			}
			return db;
		}

		override protected IDataReader Query(IDbConnection db, string sql, object[] parameters)
		{
			int i = 0;
			List<string> parameterList = new List<string> (parameters.Length);
			sql = Regex.Replace (sql, "\\!|\\?", delegate(Match match) {
				object parameter = parameters [i++];
				if (match.Value == "!") {
					return Regex.Replace (parameter.ToString(), "[^a-zA-Z0-9\\-_=<>]", "");
				}
				/*if (is_array($parameter)) return '('.implode(',',array_map(function($v) use (&$db) {
					return "'".mysqli_real_escape_string($db,$v)."'";
				},$parameter)).')';
				if (is_object($parameter) && $parameter->type=='base64') {
					return "x'".bin2hex(base64_decode($parameter->data))."'";
				}
				if ($parameter===null) return 'NULL';*/
				parameterList.Add (parameter);
				return "@_" + (parameterList.Count - 1);
			});
			MySqlCommand command = new MySqlCommand (sql, (MySqlConnection)db);
			for (i = 0; i < parameterList.Count; i++) {
				command.Parameters.AddWithValue ("@_" + i, parameterList [i]);
			}
			//DEBUG
			_context.Response.Write (sql + "\n");
			return command.ExecuteReader ();
		}

		override protected string GetDefaultCharset()
		{
			return "utf8";
		}
	}

	/*class MsSQL_CRUD_API: REST_CRUD_API
	{
		// interfaces that the MySql libraries implement, namely IDbConnection, IDbCommand, IDataParameters,

		public MsSQL_CRUD_API(HttpContext context, Config config): base(context,config)
		{
		}

		override protected IDbConnection ConnectDatabase(string hostname,string username,string password,string database,string port,string socket,string charset)
		{
			string connectionString =
				"Server=" + hostname + ";" +
				"Database=" + database + ";" +
				"User ID=" + username + ";" +
				"Password=" + password + ";";
			return new SqlConnection(connectionString);
		}

		override protected IDataReader Query(IDbConnection db, string sql, object[] parameters)
		{
		}

		override protected string GetDefaultCharset()
		{
			return "utf8";
		}
	}*/

	abstract class REST_CRUD_API
	{
		protected Dictionary<string,string> _queries;
		protected Settings _settings;

		protected HttpContext _context;

		protected class Settings
		{
			public string Method;
			public string Request;
			public NameValueCollection Get;
			public System.IO.Stream Post;
			public string Database;
			public TableAuthorizerDelegate TableAuthorizer;
			public ColumnAuthorizerDelegate ColumnAuthorizer;
			public InputSanitizerDelegate InputSanitizer;
			public InputValidatorDelegate InputValidator;
			public IDbConnection Db;
		}

		protected class Parameters
		{
			public string Action;
			public string Database;
			public string[] Tables;
			public string[] Key;
			public string Callback;
			public string Page;
			public string[] Filters;
			public string Satisfy;
			public string Columns;
			public string Order;
			public string Transform;
			public IDbConnection Db;
			public string Input;
			public string Collect;
			public string Select;

		}

		protected void ExitWith404(string type)
		{
			if (_context != null) {
				_context.Response.ContentType = null;
				_context.Response.StatusCode = 404;
				_context.Response.Write ("Not found (" + type + ")");
				_context.Response.End ();
			} else {
				throw new Exception("Not found ("+type+")");
			}
		}

		protected string ParseRequestParameter(ref string request, string characters)
		{	
			if (string.IsNullOrEmpty(request)) return null;
			int pos = request.IndexOf('/');
			string value = pos>0?request.Substring(0,pos):request;
			request = pos>0?request.Substring(pos+1):string.Empty;
			if (string.IsNullOrEmpty(characters)) return value;
			return Regex.Replace(value, "/[^"+characters+"]/", "");        
		}

		protected string ParseGetParameter(NameValueCollection get, string name, string characters)
		{
			string value = get[name];
			if (string.IsNullOrEmpty(value)) return value;
			if (string.IsNullOrEmpty(characters)) return value;
			return Regex.Replace(value, "/[^"+characters+"]/", "");        
		}

		protected string[] ParseGetParameterArray(NameValueCollection get, string name, string characters)
		{
			string value = get[name];
			string[] values = null;
			if (value == null) values = get.GetValues (name + "[]");
			else values = new string[] { value };
			if (values == null) return null;
			if (string.IsNullOrEmpty(characters)) return values;
			for (int i = 0; i < values.Length; i++) {
				values[i] = Regex.Replace(values[i], "/[^"+characters+"]/", "");
			}
			return values;
		}

		protected string MapMethodToAction(string method,string key)
		{
			switch (method) {
				case "OPTIONS": return "headers";
				case "GET": return string.IsNullOrWhiteSpace(key)?"list":"read";
				case "PUT": return "update";
				case "POST": return "create";
				case "DELETE": return "delete";
				default: ExitWith404("method"); break;
			}
			return null;
		}

		protected string[] ProcessTablesParameter(string database, string tables, string action, IDbConnection db) {
			string blacklist = "[information_schema][mysql][sys][pg_catalog]";
			if (blacklist.Contains("["+database.ToLower()+"]")) return new string[]{};
			string[] tableArray = tables.Split(',');
			List<string> tableList = new List<string> (tableArray.Length);
			foreach (string table in tableArray) {
				IDataReader reader = Query (db, _queries ["reflect_table"], new string[]{ table, database });
				while (reader.Read()) {
					tableList.Add (reader.GetString (0));
					if (action=="list") break;
				}
				reader.Close ();
			}
			if (tableList.Count==0) ExitWith404("entity");
			return tableList.ToArray();
		}

		protected string FindSinglePrimaryKey(string[] tables, string database, IDbConnection db) {
			return "id";
		}

		protected string[] ProcessKeyParameter(string key, string[] tables, string database, IDbConnection db) {
			if (string.IsNullOrEmpty (key))	return null;
			int count = 0;
			string field = null;
			IDataReader reader = Query (db, _queries ["reflect_pk"], new string[]{ tables[0], database });
			while (reader.Read()) {
				count++;
				field = reader.GetString (0);
			}
			reader.Close ();
			if (count!=1 || field==null) ExitWith404("1pk");
			return new string[]{ key, field };
		}

		protected Parameters GetParameters(Settings settings)
		{
			Parameters parameters = new Parameters {};

			string tables        = ParseRequestParameter(ref settings.Request, "a-zA-Z0-9\\-_*,");
			string key           = ParseRequestParameter(ref settings.Request, "a-zA-Z0-9\\-,"); // auto-increment or uuid
			parameters.Action    = MapMethodToAction(settings.Method,key);
			parameters.Callback  = ParseGetParameter(settings.Get, "callback", "a-zA-Z0-9\\-_");
			parameters.Page      = ParseGetParameter(settings.Get, "page", "0-9,");
			parameters.Filters   = ParseGetParameterArray(settings.Get, "filter", null);
			parameters.Satisfy   = ParseGetParameter(settings.Get, "satisfy", "a-z");
			parameters.Columns   = ParseGetParameter(settings.Get, "columns", "a-zA-Z0-9\\-_,");
			parameters.Order     = ParseGetParameter(settings.Get, "order", "a-zA-Z0-9\\-_*,");
			parameters.Transform = ParseGetParameter(settings.Get, "transform", "1");
			parameters.Db        = settings.Db;

			parameters.Tables    = ProcessTablesParameter(settings.Database,tables,parameters.Action,settings.Db);
			parameters.Key       = ProcessKeyParameter(key,parameters.Tables,settings.Database,settings.Db);


		/*
			foreach ($filters as &$filter) $filter = $this->processFilterParameter($filter,$db);
			if ($columns) $columns = explode(',',$columns);
			$page      = $this->processPageParameter($page);
			$satisfy   = ($satisfy && strtolower($satisfy)=='any')?'any':'all';
			$order     = $this->processOrderParameter($order);
		*/


		/*
			// reflection
			list($collect,$select) = $this->findRelations($tables,$database,$db);
			$fields = $this->findFields($tables,$collect,$select,$columns,$database,$db);
			
			// permissions
			if ($table_authorizer) $this->applyTableAuthorizer($table_authorizer,$action,$database,$tables);
			if ($column_authorizer) $this->applyColumnAuthorizer($column_authorizer,$action,$database,$fields);

			if ($post) {
				// input
				$context = $this->retrieveInput($post);
				$input = $this->filterInputByColumns($context,$fields[$tables[0]]);
				
				if ($input_sanitizer) $this->applyInputSanitizer($input_sanitizer,$action,$database,$tables[0],$input,$fields[$tables[0]]);
				if ($input_validator) $this->applyInputValidator($input_validator,$action,$database,$tables[0],$input,$fields[$tables[0]],$context);

				$this->convertBinary($input,$fields[$tables[0]]);
			}
			
		 */
			//DEBUG
			//_context.Response.Write (parameters.Action+" - "+parameters.Tables[0]+" - "+parameters.Key+" - "+parameters.Callback);


			return parameters;
		}

		protected void ReadCommand(Parameters parameters)
		{

		}

		protected void CreateCommand(Parameters parameters)
		{

		}

		protected void UpdateCommand(Parameters parameters)
		{

		}

		protected void DeleteCommand(Parameters parameters)
		{

		}

		protected void ListCommand(Parameters parameters)
		{

		}

		protected void HeadersCommand(Parameters parameters)
		{
			Dictionary<string,string> headers = new Dictionary<string,string>() {
				{ "Access-Control-Allow-Headers", "Content-Type" },
				{ "Access-Control-Allow-Methods", "OPTIONS, GET, PUT, POST, DELETE" },
				{ "Access-Control-Max-Age", "1728000" }
			};
			if (_context != null) {
				foreach (KeyValuePair<string, string> header in headers) {
					_context.Response.AddHeader (header.Key, header.Value);
				}
			} else {
				JavaScriptSerializer json = new JavaScriptSerializer ();
				_context.Response.Write(json.Serialize (headers));
			}
		}

		public REST_CRUD_API(HttpContext context, Config config)
		{
			_context = context;

			// defaults
			if (config.Method == null) {
				config.Method = context.Request.HttpMethod;
			}
			if (config.Request == null) {
				config.Request = context.Request.PathInfo;
			}
			if (config.Get == null) {
				config.Get = context.Request.QueryString;
			}
			if (config.Post == null) {
				config.Post = context.Request.InputStream;
			}
			if (config.Charset == null) {
				config.Charset = this.GetDefaultCharset();
			}

			// connect
			String request = config.Request.Trim ('/');

			if (config.Database == null) {
				config.Database = ParseRequestParameter(ref request, "a-zA-Z0-9\\-_");
			}
			if (config.Db == null) {
				config.Db = ConnectDatabase(config.Hostname,config.Username,config.Password,config.Database,config.Port,config.Socket,config.Socket);
			}

			_settings = new Settings{
				Method = config.Method,
				Request = request,
				Get = config.Get,
				Post = config.Post,
				Database = config.Database,
				TableAuthorizer = config.TableAuthorizer,
				ColumnAuthorizer = config.ColumnAuthorizer,
				InputSanitizer = config.InputSanitizer,
				InputValidator = config.InputValidator,
				Db = config.Db
			};

		}

		public void ExecuteCommand()
		{
			if (_context != null) {
				_context.Response.AddHeader ("Access-Control-Allow-Origin", "*");
			}
			Parameters parameters = this.GetParameters(_settings);
			switch(parameters.Action) {
				case "list":    ListCommand(parameters);    break;
				case "read":    ReadCommand(parameters);    break;
				case "create":  CreateCommand(parameters);  break;
				case "update":  UpdateCommand(parameters);  break;
				case "delete":  DeleteCommand(parameters);  break;
				case "headers": HeadersCommand(parameters); break;
			}
		}

		// abstract 

		abstract protected IDbConnection ConnectDatabase (string hostname, string username, string password, string database, string port, string socket, string charset);

		abstract protected IDataReader Query (IDbConnection db, string sql, object[] parameters);

		abstract protected string GetDefaultCharset ();

	}

	public class Handler: System.Web.IHttpHandler
	{
		public void ProcessRequest (HttpContext context)
		{
			MySQL_CRUD_API api = new MySQL_CRUD_API (context,new Config{
				Hostname = "localhost",
				Username = "root",
				Password = "",
				Database = "php-crud-api"
				//Database = null
			});
			api.ExecuteCommand ();
		}

		public bool IsReusable {
			get {
				return false;
			}
		}
	}
}