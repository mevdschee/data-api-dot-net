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
	delegate bool RecordFilterDelegate     (string action, string database, string table);
	delegate bool ColumnAuthorizerDelegate (string action, string database, string table, string column);
	delegate bool TenantFunctionDelegate   (string action, string database, string table, string column);
	delegate bool InputSanitizerDelegate   (string action, string database, string table, string column, string type, object value);
	delegate bool InputValidatorDelegate   (string action, string database, string table, string column, string type, object value, NameValueCollection context);

	struct Filter
	{
		public string Field;
		public string Comparator;
		public object Value;
	}

	struct FilterSet
	{
		public List <Filter> Or;
		public List <Filter> And;
	}

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
		public RecordFilterDelegate RecordFilter;
		public ColumnAuthorizerDelegate ColumnAuthorizer;
		public TenantFunctionDelegate TenantFunction;
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
				"Pooling=true; CharSet=utf8;";
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
			List<object> parameterList = new List<object> (parameters.Length);
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
			//_context.Response.Write (sql + "\n");
			return command.ExecuteReader ();
		}

		override protected string LikeEscape(string s) {
			return s.Replace("%",@"\%").Replace("_",@"\_");
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
			public string[] Page;
			public Dictionary<string, FilterSet> Filters;
			public string Fields;
			public string[] Order;
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

		protected string[] ProcessOrderParameter(string order) {
			if (order == null) return null;
			string[] result = new string[2] { order, "ASC"} ;
			if (order.IndexOf(',')>=0) result = order.Split(new char[]{','},2);
			if (result[0].Length == 0)	return null;
			result[1] = result[1].ToUpper()=="DESC"?"DESC":"ASC";
			return result;
		}

		protected Filter ConvertFilter(string field, string comparator, string value) {
			Filter filter = new Filter();
			filter.Field = field;
			filter.Comparator = null;
			filter.Value = value;
			switch (comparator.ToLower()) {
				case "cs": filter.Comparator = "LIKE"; filter.Value = '%'+LikeEscape(value)+'%'; break;
				case "sw": filter.Comparator = "LIKE"; filter.Value = LikeEscape(value)+'%'; break;
				case "ew": filter.Comparator = "LIKE"; filter.Value = '%'+LikeEscape(value); break;
				case "eq": filter.Comparator = "="; break;
				case "ne": filter.Comparator = "<>"; break;
				case "lt": filter.Comparator = "<"; break;
				case "le": filter.Comparator = "<="; break;
				case "ge": filter.Comparator = ">="; break;
				case "gt": filter.Comparator = ">"; break;
				case "in": filter.Comparator = "IN"; filter.Value = value.Split(','); break;
			}
			return filter;
		}

		protected List <Filter> ConvertFilters(string[] filters) {
			List <Filter> results = new List<Filter>(filters.Length);
			if (filters!=null) {
				for (int i=0;i<filters.Length;i++) {
					string[] filter = filters[i].Split(new char[]{','},3);
					if (filter.Length == 3) {
						results.Add(ConvertFilter(filter[0],filter[1],filter[2]));
					}
				}
			}
			return results;
		}

		protected Dictionary<string,FilterSet> ProcessFiltersParameter(string[] tables,string satisfy,string[] filters) {
			Dictionary<string,FilterSet> results = new Dictionary<string,FilterSet> ();
			FilterSet filterSet = new FilterSet();
			List <Filter> result = ConvertFilters(filters);
			if (result==null) return results;
			if (satisfy.ToLower () == "any") filterSet.Or = result;
			else filterSet.And = result;
			results.Add(tables[0],filterSet);
			return results;
		}

		protected string[] ProcessPageParameter(string page) {
			if (page == null) return null;
			string[] result = new string[2] { page, "20"} ;
			if (page.IndexOf(',')>0) result = page.Split(new char[]{','},2);
			int number, size;
			if (!Int32.TryParse (result [0], out number) || !Int32.TryParse (result [1], out size)) {
				return null;
			}
			result [0] = Convert.ToString ((number - 1) * size);
			return result;
		}

		protected Parameters GetParameters(Settings settings)
		{
			Parameters parameters = new Parameters {};

			string tables        = ParseRequestParameter(ref settings.Request, "a-zA-Z0-9\\-_,");
			string key           = ParseRequestParameter(ref settings.Request, "a-zA-Z0-9\\-_,"); // auto-increment or uuid
			parameters.Action    = MapMethodToAction(settings.Method,key);
			parameters.Callback  = ParseGetParameter(settings.Get, "callback", "a-zA-Z0-9\\-_");
			string page          = ParseGetParameter(settings.Get, "page", "0-9,");
			string[] filters     = ParseGetParameterArray(settings.Get, "filter", null);
			string satisfy       = ParseGetParameter(settings.Get, "satisfy", "a-z");
			string columns       = ParseGetParameter(settings.Get, "columns", "a-zA-Z0-9\\-_,");
			string order         = ParseGetParameter(settings.Get, "order", "a-zA-Z0-9\\-_,");
			parameters.Transform = ParseGetParameter(settings.Get, "transform", "1");
			parameters.Db        = settings.Db;

			parameters.Tables    = ProcessTablesParameter(settings.Database,tables,parameters.Action,settings.Db);
			parameters.Key       = ProcessKeyParameter(key,parameters.Tables,settings.Database,settings.Db);
			parameters.Filters   = ProcessFiltersParameter(parameters.Tables,satisfy,filters);
			parameters.Page      = ProcessPageParameter(page);
			parameters.Order     = ProcessOrderParameter(order);

		/*
			// reflection
			list($collect,$select) = $this->findRelations($tables,$database,$db);
			parameters.Fields = $this->findFields($tables,$collect,$select,$columns,$database,$db);
			
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
			_context.Response.Write (parameters.Action+" - "+parameters.Tables[0]+" - "+parameters.Key+" - "+parameters.Callback);


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

		abstract protected string LikeEscape(string s);

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