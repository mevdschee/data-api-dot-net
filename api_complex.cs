using System;
using System.Web;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text.RegularExpressions;

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
		public int? Port;
		public string Socket;
		public string Charset;
		// callbacks with their default behavior
		public TableAuthorizerDelegate TableAuthorizer;
		public ColumnAuthorizerDelegate ColumnAuthorizer;
		public InputSanitizerDelegate InputSanitizer;
		public InputValidatorDelegate InputValidator;
		// dependencies (added for unit testing):
		public string Db;
		public string Method;
		public string Request;
		public NameValueCollection Get;
		public System.IO.Stream Post;
	}

	class MySQL_CRUD_API: REST_CRUD_API
	{
		public MySQL_CRUD_API(HttpContext context, Config config): base(context,config)
		{
		}

		override protected string GetDefaultCharset()
		{
			return "utf8";
		}
	}

	abstract class REST_CRUD_API
	{
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
			public object Db;
		}

		protected class Parameters
		{
			public string Action;
			public string Database;
			public string Table;
			public string Key;
			public string Callback;
			public string Page;
			public string Filters;
			public string Satisfy;
			public string Columns;
			public string Order;
			public string Transform;
			public string Db;
			public string Input;
			public string Collect;
			public string Select;

		}

		protected Parameters GetParameters(Settings settings)
		{
			Parameters parameters = new Parameters {};

			/*
			$table     = $this->parseRequestParameter($request, 'a-zA-Z0-9\-_*,', false);
			$key       = $this->parseRequestParameter($request, 'a-zA-Z0-9\-,', false); // auto-increment or uuid
			$action    = $this->mapMethodToAction($method,$key);
			$callback  = $this->parseGetParameter($get, 'callback', 'a-zA-Z0-9\-_', false);
			$page      = $this->parseGetParameter($get, 'page', '0-9,', false);
			$filters   = $this->parseGetParameterArray($get, 'filter', false, false);
			$satisfy   = $this->parseGetParameter($get, 'satisfy', 'a-z', 'all');
			$columns   = $this->parseGetParameter($get, 'columns', 'a-zA-Z0-9\-_,', false);
			$order     = $this->parseGetParameter($get, 'order', 'a-zA-Z0-9\-_*,', false);
			$transform = $this->parseGetParameter($get, 'transform', '1', false);

			$table    = $this->processTableParameter($database,$table,$db);
			$key      = $this->processKeyParameter($key,$table,$database,$db);
			foreach ($filters as &$filter) $filter = $this->processFilterParameter($filter,$db);
			if ($columns) $columns = explode(',',$columns);
			$page     = $this->processPageParameter($page);
			$order    = $this->processOrderParameter($order);

			if (empty($table)) $this->exitWith404('entity');

			// reflection
			list($collect,$select) = $this->findRelations($table,$database,$db);
			$columns = $this->findFields($table,$collect,$select,$columns,$database,$db);

			// permissions
			if ($table_authorizer) $this->applyTableAuthorizer($table_authorizer,$action,$database,$table);
			if ($column_authorizer) $this->applyColumnAuthorizer($column_authorizer,$action,$database,$columns);

			// input
			$context = $this->retrieveInput($post);
			if (!empty($context)) $input = $this->limitInputFields($context,$columns[$table[0]]);

			if ($input_sanitizer) $this->applyInputSanitizer($input_sanitizer,$action,$database,$table[0],$input,$columns[$table[0]]);
			if ($input_validator) $this->applyInputValidator($input_validator,$action,$database,$table[0],$input,$columns[$table[0]],$context);

			if (!empty($input)) $input = $this->convertBinary($input,$columns[$table[0]]);
			 */

			return parameters;
		}

		protected HttpContext _context;

		protected Settings _settings;

		protected void ListCommand(Parameters parameters)
		{

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

		protected string ParseRequestParameter(ref string request, string characters)
		{	if (string.IsNullOrEmpty(request)) return null;
			int pos = request.IndexOf('/');
			string value = pos>0?request.Substring(0,pos):request;
			request = pos>0?request.Substring(pos+1):string.Empty;
			if (string.IsNullOrEmpty(characters)) return value;
			return Regex.Replace(value, "/[^"+characters+"]/", "");        
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
				config.Database = ParseRequestParameter(ref request, "a-zA-Z0-9-_,");
				_context.Response.Write (config.Database);
			}
			if (config.Db == null) {
				//config.Db = ConnectDatabase(config.Hostname,config.Username,config.Password,config.Database,config.Port,config.Socket,config.Socket);
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
			_context.Response.AddHeader ("Access-Control-Allow-Origin", "*");
			Parameters parameters = this.GetParameters(_settings);
			switch(parameters.Action) {
				case "list":   ListCommand(parameters);   break;
				case "read":   ReadCommand(parameters);   break;
				case "create": CreateCommand(parameters); break;
				case "update": UpdateCommand(parameters); break;
				case "delete": DeleteCommand(parameters); break;
			}
			_context.Response.Write ("OK");
		}

		// abstract 

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
				//Database = "mysql_crud_api"
				Database = null
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