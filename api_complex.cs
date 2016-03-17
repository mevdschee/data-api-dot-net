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

	struct Field
	{
		public string Name;
		public string Type;
	}

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
		public string DbEngine;
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
		public DatabaseInterface Db;
		public string Method;
		public string Request;
		public NameValueCollection Get;
		public System.IO.Stream Post;
	}

	interface DatabaseInterface
	{
		string GetSql(string name);
		void Connect(string hostname,string username,string password,string database,string port,string socket,string charset);
		IDataReader Query(string sql,object[] parameters);
		Dictionary<string,object> FetchAssoc(IDataReader reader);
		//function fetchRow($result);
		//function insertId($result);
		//function affectedRows($result);
		bool Close(IDataReader reader);
		List<Field> FetchFields(IDataReader reader);
		//function addLimitToSql($sql,$limit,$offset);
		string LikeEscape(string s);
		//function isBinaryType($field);
		//function base64Encode($string);
		string GetDefaultCharset();
	}

	class MySQL: DatabaseInterface
	{
		protected IDbConnection _db;
		protected HttpContext _context;
		protected Dictionary<string,string> _queries;

		// interfaces that the MySql libraries implement, namely IDbConnection, IDbCommand, IDataParameters,

		public MySQL(HttpContext context)
		{
			_context = context;
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

		public string GetSql(string name)
		{
			return _queries.ContainsKey(name) ? _queries[name] : null;
		}

		public void Connect(string hostname,string username,string password,string database,string port,string socket,string charset)
		{
			MySqlConnection db=null;
			string connectionString =
				"Server=" + hostname + ";" +
				"Database=" + database + ";" +
				"User ID=" + username + ";" +
				"Password=" + password + ";" +
				"CharSet=" + charset + ";" +
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
			_db = db;
		}

		public IDataReader Query(string sql, object[] parameters)
		{
			int i = 0;
			List<object> parameterList = new List<object> (parameters.Length);
			sql = Regex.Replace (sql, "\\!|\\?", delegate(Match match) {
				object parameter = parameters [i++];
				if (match.Value == "!") {
					return Regex.Replace (parameter.ToString(), "[^a-zA-Z0-9\\-_=<> ]", "");
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
			MySqlCommand command = new MySqlCommand (sql, (MySqlConnection)_db);
			for (i = 0; i < parameterList.Count; i++) {
				command.Parameters.AddWithValue ("@_" + i, parameterList [i]);
			}
			//DEBUG
			//_context.Response.Write (sql + "\n");
			return command.ExecuteReader ();
		}

		public Dictionary<string,object> FetchAssoc(IDataReader reader)
		{
			Dictionary<string, object> obj = new Dictionary<string, object>();
			for (int i=0;i<reader.FieldCount;i++) {
				obj.Add(reader.GetName(i), reader.GetValue(i));
			}
			return obj;
		}

		public bool Close(IDataReader reader)
		{
			reader.Close ();
			return true;
		}

		public List<Field> FetchFields(IDataReader reader)
		{
			List<Field> fields = new List<Field>();
			DataTable schema = reader.GetSchemaTable();
			foreach (DataRow row in schema.Rows)
			{
				Field field = new Field();
				field.Name = row ["ColumnName"].ToString();
				field.Type = row ["DataType"].ToString();
				fields.Add(field);
			}
			return fields;
		}

		public string LikeEscape(string s)
		{
			return s.Replace("%",@"\%").Replace("_",@"\_");
		}

		public string GetDefaultCharset()
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

	class PHP_CRUD_API
	{
		protected DatabaseInterface _db;
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
		}

		protected class Parameters
		{
			public string Action;
			public string Database;
			public List<string> Tables;
			public string[] Key;
			public string Callback;
			public string[] Page;
			public Dictionary<string, FilterSet> Filters;
			public Dictionary<string,Dictionary<string,Field>> Fields;
			public string[] Order;
			public string Transform;
			public string Input;
			public Dictionary<string,Dictionary<string,List<string>>> Collect;
			public Dictionary<string,Dictionary<string,List<string>>> Select;

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
			return Regex.Replace(value, "[^"+characters+"]", "");        
		}

		protected string ParseGetParameter(NameValueCollection get, string name, string characters)
		{
			string value = get[name];
			if (string.IsNullOrEmpty(value)) return value;
			if (string.IsNullOrEmpty(characters)) return value;
			return Regex.Replace(value, "[^"+characters+"]", "");        
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
				values[i] = Regex.Replace(values[i], "[^"+characters+"]", "");
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

		protected List<string> ProcessTableAndIncludeParameter(string database, string table, string include, string action) {
			List<string> blacklist = new List<string>{ "information_schema", "mysql", "sys", "pg_catalog" };
			if (blacklist.Contains(database.ToLower())) return new List<string>();
			List<string> tableList = new List<string> ();
			IDataReader reader = _db.Query (_db.GetSql("reflect_table"), new string[]{ table, database });
			while (reader.Read()) {
				tableList.Add (reader.GetString (0));
			}
			_db.Close (reader);
			if (tableList.Count==0) ExitWith404("entity");
			if (action=="list") {
				foreach (string table2 in include.Split(',')) {
					reader = _db.Query (_db.GetSql("reflect_table"), new string[]{ table2, database });
					while (reader.Read()) {
						tableList.Add (reader.GetString (0));
					}
					_db.Close (reader);
				}
			}
			return tableList;
		}

		protected string FindSinglePrimaryKey(List<string> tables, string database) {
			return "id";
		}


		protected void StartOutput(string callback) {
			if (callback!=null) {
				if (_context != null) {
					_context.Response.AddHeader("Content-Type","application/javascript");
				}
				_context.Response.Write(callback+"(");
			} else {
				if (_context != null) {
					_context.Response.AddHeader("Content-Type","application/json");
				}
			}
		}

		protected void EndOutput(string callback) {
			if (callback!=null) {
				_context.Response.Write(");");
			}
		}

		protected string[] ProcessKeyParameter(string key, List<string> tables, string database) {
			if (string.IsNullOrEmpty (key))	return null;
			int count = 0;
			string field = null;
			IDataReader r = _db.Query (_db.GetSql("reflect_pk"), new string[]{ tables[0], database });
			while (r.Read()) {
				count++;
				field = r.GetString (0);
			}
			_db.Close (r);
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
				case "cs": filter.Comparator = "LIKE"; filter.Value = '%'+_db.LikeEscape(value)+'%'; break;
				case "sw": filter.Comparator = "LIKE"; filter.Value = _db.LikeEscape(value)+'%'; break;
				case "ew": filter.Comparator = "LIKE"; filter.Value = '%'+_db.LikeEscape(value); break;
				case "eq": filter.Comparator = "="; break;
				case "ne": filter.Comparator = "<>"; break;
				case "lt": filter.Comparator = "<"; break;
				case "le": filter.Comparator = "<="; break;
				case "ge": filter.Comparator = ">="; break;
				case "gt": filter.Comparator = ">"; break;
				case "in": filter.Comparator = "IN"; filter.Value = value.Split(','); break;
				case "ni": filter.Comparator = "NOT IN"; filter.Value = value.Split(','); break;
				case "is": filter.Comparator = "IS"; filter.Value = null; break;
				case "no": filter.Comparator = "IS NOT"; filter.Value = null; break;
			}
			return filter;
		}

		protected List <Filter> ConvertFilters(string[] filters) {
			List <Filter> results = new List<Filter>();
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

		protected Dictionary<string,FilterSet> ProcessFiltersParameter(List<string> tables,string satisfy,string[] filters) {
			Dictionary<string,FilterSet> results = new Dictionary<string,FilterSet> ();
			FilterSet filterSet = new FilterSet();
			List <Filter> result = ConvertFilters(filters);
			if (result==null) return results;
			if (satisfy!=null && satisfy.ToLower () == "any") filterSet.Or = result;
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

		protected Dictionary<string,object> RetrieveObject(string[] key,Dictionary<string,Dictionary<string,Field>> fields,Dictionary<string,FilterSet> filters,List<string> tables) {
			if (key==null) return null;
			string table = tables[0];
			string sql = "SELECT ";
			sql += "\""+String.Join("\",\"",fields[table].Keys)+"\"";
			sql += " FROM \"!\"";
			object[] parameters = new object[]{ (object)table };
			if (!filters.ContainsKey(table)) filters[table] = new FilterSet();
			if (filters [table].Or == null) {
				FilterSet f = filters [table];
				f.Or = new List<Filter>();
				filters [table] = f;
			}
			filters[table].Or.Add(new Filter(){ Field = key[1], Comparator = "=", Value = key[0] });
			AddWhereFromFilters(filters[table],ref sql,ref parameters);
			Dictionary<string,object> obj = null;
			IDataReader reader = _db.Query(sql,parameters);
			if (reader.Read()) {
				obj = _db.FetchAssoc(reader);
				foreach (Field field in fields[table].Values) {
					//if (_db.IsBinaryType(field) && obj.ContainsKey(field.Name)) {
					//	obj[field.Name] = _db.Base64Encode(obj[field.Name]);
					//}
				}
				_db.Close(reader);
			}
			return obj;
		}

		protected void FindRelations(ref List<string> tables,ref Dictionary<string,Dictionary<string,List<string>>> collect,ref Dictionary<string,Dictionary<string,List<string>>> select,string database) {
			collect = new Dictionary<string,Dictionary<string,List<string>>> ();
			select = new Dictionary<string,Dictionary<string,List<string>>> ();
			List<string> tableset = new List<string>();

			while (tables.Count>1) {
				string table0 = tables[0];
				tables.RemoveAt (0);
				tableset.Add(table0);

				IDataReader r;

				r = _db.Query(_db.GetSql("reflect_belongs_to"),new object[]{ table0, tables.ToArray(), database, database });
				while (r.Read()) {
					collect[r.GetString(0)][r.GetString(1)]=new List<string>();
					select[r.GetString(2)][r.GetString(3)]=new List<string>{r.GetString(0),r.GetString(1)};
					if (!tableset.Contains(r.GetString(0))) tableset.Add(r.GetString(0));
				}
				_db.Close (r);

				r = _db.Query(_db.GetSql("reflect_has_many"),new object[]{ tables.ToArray(), table0, database, database });
				while (r.Read()) {
					collect[r.GetString(2)][r.GetString(3)]=new List<string>();
					select[r.GetString(0)][r.GetString(1)]=new List<string>{r.GetString(2),r.GetString(3)};
					if (!tableset.Contains(r.GetString(2))) tableset.Add(r.GetString(2));
				}
				_db.Close (r);

				r = _db.Query(_db.GetSql("reflect_habtm"),new object[]{ database, database, database, database, table0, tables.ToArray() });
				while (r.Read()) {
					collect[r.GetString(2)][r.GetString(3)]=new List<string>();
					select[r.GetString(0)][r.GetString(1)]=new List<string>{r.GetString(2),r.GetString(3)};
					collect[r.GetString(4)][r.GetString(5)]=new List<string>();
					select[r.GetString(6)][r.GetString(7)]=new List<string>{r.GetString(4),r.GetString(5)};
					if (!tableset.Contains(r.GetString(2))) tableset.Add(r.GetString(2));
					if (!tableset.Contains(r.GetString(4))) tableset.Add(r.GetString(4));
				}
				_db.Close (r);
			}
			tableset.Add(tables[0]);
			tables = tableset;
		}

		protected Dictionary<string,Dictionary<string,Field>> FindFields(List<string> tables,string columns,string database) {
			Dictionary<string,Dictionary<string,Field>> fields = new Dictionary<string,Dictionary<string,Field>>();
			for (int i=0;i<tables.Count;i++) {
				string table = tables[i];
				fields[table] = FindTableFields(table,database);
				if (i==0) fields[table] = FilterFieldsByColumns(fields[table],columns);
			}
			return fields;
		}

		protected Dictionary<string,Field> FilterFieldsByColumns(Dictionary<string,Field> fields,string columns) {
			Dictionary<string,Field> result = new Dictionary<string,Field>(fields);
			if (columns!=null) {
				List<string> cols = new List<string>(columns.Split(new char[]{','}));
				foreach (KeyValuePair<string,Field> kv in fields) {
					if (!cols.Contains(kv.Key)) {
						result.Remove(kv.Key);
					}
				}
			}
			return result;
		}

		protected Dictionary<string,Field> FindTableFields(string table,string database) {
			Dictionary<string,Field> fields = new Dictionary<string,Field>();
			IDataReader reader = _db.Query("SELECT * FROM \"!\" WHERE 1=2;", new string[]{ table });
			foreach (Field field in _db.FetchFields(reader))
			{
				fields [field.Name] = field;
			}
			_db.Close (reader);
			return fields;
		}

		protected Parameters GetParameters(Settings settings)
		{
			Parameters parameters = new Parameters {};

			string table         = ParseRequestParameter(ref settings.Request, "a-zA-Z0-9\\-_");
			string key           = ParseRequestParameter(ref settings.Request, "a-zA-Z0-9\\-_"); // auto-increment or uuid
			parameters.Action    = MapMethodToAction(settings.Method,key);
			string include       = ParseGetParameter(settings.Get, "include", "a-zA-Z0-9\\-_,");
			parameters.Callback  = ParseGetParameter(settings.Get, "callback", "a-zA-Z0-9\\-_");
			string page          = ParseGetParameter(settings.Get, "page", "0-9,");
			string[] filters     = ParseGetParameterArray(settings.Get, "filter", null);
			string satisfy       = ParseGetParameter(settings.Get, "satisfy", "a-z");
			string columns       = ParseGetParameter(settings.Get, "columns", "a-zA-Z0-9\\-_,");
			string order         = ParseGetParameter(settings.Get, "order", "a-zA-Z0-9\\-_,");
			parameters.Transform = ParseGetParameter(settings.Get, "transform", "t1");
			parameters.Database  = settings.Database;

			parameters.Tables    = ProcessTableAndIncludeParameter(parameters.Database,table,include,parameters.Action);
			parameters.Key       = ProcessKeyParameter(key,parameters.Tables,parameters.Database);
			parameters.Filters   = ProcessFiltersParameter(parameters.Tables,satisfy,filters);
			parameters.Page      = ProcessPageParameter(page);
			parameters.Order     = ProcessOrderParameter(order);

			// reflection
			FindRelations(ref parameters.Tables,ref parameters.Collect,ref parameters.Select,parameters.Database);
			parameters.Fields = FindFields(parameters.Tables,columns,parameters.Database);

			/*

			// permissions
			if ($table_authorizer) $this->applyTableAuthorizer($table_authorizer,$action,$database,$tables);
			if ($record_filter) $this->applyRecordFilter($record_filter,$action,$database,$tables,$filters);
			if ($column_authorizer) $this->applyColumnAuthorizer($column_authorizer,$action,$database,$fields);
			if ($tenancy_function) $this->applyTenancyFunction($tenancy_function,$action,$database,$fields,$filters);

			if ($post) {
				// input
				$context = $this->retrieveInput($post);
				$input = $this->filterInputByFields($context,$fields[$tables[0]]);

				if ($tenancy_function) $this->applyInputTenancy($tenancy_function,$action,$database,$tables[0],$input,$fields[$tables[0]]);
				if ($input_sanitizer) $this->applyInputSanitizer($input_sanitizer,$action,$database,$tables[0],$input,$fields[$tables[0]]);
				if ($input_validator) $this->applyInputValidator($input_validator,$action,$database,$tables[0],$input,$fields[$tables[0]],$context);

				$this->convertBinary($input,$fields[$tables[0]]);
			}
			
		 */
			//DEBUG
			//_context.Response.Write (parameters.Action+" - "+parameters.Tables[0]+" - "+String.Join(",",parameters.Key)+" - "+parameters.Callback+" - "+String.Join(",",parameters.Page));

			return parameters;
		}

		protected void AddWhereFromFilters(FilterSet filters,ref string sql,ref object[] parameters) {
			bool first = true;
			List<object> parameterList = new List<object>(parameters);
			if (filters.Or != null) {
				first = false;
				sql += " WHERE (";
				for (int i=0;i<filters.Or.Count;i++) {
					Filter filter = filters.Or[i];
					sql += i==0?"":" OR ";
					sql += "\"!\" ! ?";
					parameterList.Add(filter.Field);
					parameterList.Add(filter.Comparator);
					parameterList.Add(filter.Value);
				}
				sql += ")";
			}
			if (filters.And != null) {
				for (int i=0;i<filters.And.Count;i++) {
					Filter filter = filters.And[i];
					sql += first?" WHERE ":" AND ";
					sql += "\"!\" ! ?";
					parameterList.Add(filter.Field);
					parameterList.Add(filter.Comparator);
					parameterList.Add(filter.Value);
					first = false;
				}
			}
			parameters = parameterList.ToArray();
		}

		protected void ReadCommand(Parameters parameters)
		{
			Dictionary<string,object> obj = RetrieveObject(parameters.Key,parameters.Fields,parameters.Filters,parameters.Tables);
			if (obj==null) ExitWith404("object");
			StartOutput(parameters.Callback);
			_context.Response.Write ((new JavaScriptSerializer ()).Serialize ((object)obj));
			EndOutput(parameters.Callback);
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

		public PHP_CRUD_API(HttpContext context, Config config)
		{
			_context = context;

			// defaults
			if (config.DbEngine == null) {
				config.DbEngine = "MySQL";
			}
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

			// connect
			String request = config.Request.Trim ('/');

			if (config.Database == null) {
				config.Database = ParseRequestParameter(ref request, "a-zA-Z0-9\\-_");
			}
			if (config.Db == null) {
				config.Db = (DatabaseInterface)Activator.CreateInstance (Type.GetType ("DataApiDotNet_Complex."+config.DbEngine),context);
				if (config.Charset == null) {
					config.Charset = config.Db.GetDefaultCharset();
				}
				config.Db.Connect(config.Hostname,config.Username,config.Password,config.Database,config.Port,config.Socket,config.Charset);
			}

			_db = config.Db;
			_settings = new Settings{
				Method = config.Method,
				Request = request,
				Get = config.Get,
				Post = config.Post,
				Database = config.Database,
				TableAuthorizer = config.TableAuthorizer,
				ColumnAuthorizer = config.ColumnAuthorizer,
				InputSanitizer = config.InputSanitizer,
				InputValidator = config.InputValidator
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

	}

	public class Handler: System.Web.IHttpHandler
	{
		public void ProcessRequest (HttpContext context)
		{
			PHP_CRUD_API api = new PHP_CRUD_API (context,new Config{
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