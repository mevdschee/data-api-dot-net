using System;
using System.Web;
using System.Collections.Generic;
using System.Collections.Specialized;

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
		public string Get;
		public string Post;
	}

	class Base
	{
		protected class Settings
		{
			public string Method;
			public string[] Request;
			public NameValueCollection Get;
			public string Post;
			public string Database;
			public TableAuthorizerDelegate TableAuthorizer;
			public ColumnAuthorizerDelegate ColumnAuthorizer;
			public InputSanitizerDelegate InputSanitizer;
			public InputValidatorDelegate InputValidator;
			public object Db;
		}

		protected class Parameters
		{
			public Parameters(Settings settings)
			{

			}

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

		public Base(HttpContext context, Config config)
		{
			_context = context;

			//string hostname = config.Hostname;
			//string username = config.Username;

			context.Response.Write (config.Username);

			/*isset($username)?$username:'root';
			$password = isset($password)?$password:null;
			$database = isset($database)?$database:false;
			$port = isset($port)?$port:null;
			$socket = isset($socket)?$socket:null;
			$charset = isset($charset)?$charset:'utf8';

			$callbacks['table_authorizer'] = isset($table_authorizer)?$table_authorizer:false;
			$callbacks['column_authorizer'] = isset($column_authorizer)?$column_authorizer:false;
			$callbacks['input_sanitizer'] = isset($input_sanitizer)?$input_sanitizer:false;
			$callbacks['input_validator'] = isset($input_validator)?$input_validator:false;

			$db = isset($db)?$db:null;
			$method = isset($method)?$method:$_SERVER['REQUEST_METHOD'];
			$request = isset($request)?$request:(isset($_SERVER['PATH_INFO'])?$_SERVER['PATH_INFO']:'');
			$get = isset($get)?$get:$_GET;
			$post = isset($post)?$post:'php://input';

			$request = explode('/', trim($request,'/'));

			if (!$database) {
				$database  = $this->parseRequestParameter($request, 'a-zA-Z0-9\-_,', false);
			}
			if (!$db) {
				$db = $this->connectDatabase($hostname,$username,$password,$database,$port,$socket,$charset);
			}*/

			//_settings = compact('method', 'request', 'get', 'post', 'database', 'callbacks', 'db');
		}

		public void ExecuteCommand()
		{
			_context.Response.AddHeader ("Access-Control-Allow-Origin", "*");
			Parameters parameters = new Parameters(_settings);
			switch(parameters.Action) {
				case "list":   ListCommand(parameters);   break;
				case "read":   ReadCommand(parameters);   break;
				case "create": CreateCommand(parameters); break;
				case "update": UpdateCommand(parameters); break;
				case "delete": DeleteCommand(parameters); break;
			}
		}

	}

	public class Handler: System.Web.IHttpHandler
	{

		public void ProcessRequest (HttpContext context)
		{

			// uncomment the lines below when running in stand-alone mode:
			// $api = new MySQL_CRUD_API(array(
			// 	'hostname'=>'localhost',
			//	'username'=>'xxx',
			//	'password'=>'xxx',
			//	'database'=>'xxx',
			// 	'charset'=>'utf8'
			// ));
			// $api->executeCommand();
			// For Microsoft SQL Server use:
			// $api = new MsSQL_CRUD_API(array(
			// 	'hostname'=>'(local)',
			// 	'username'=>'',
			// 	'password'=>'',
			// 	'database'=>'xxx',
			// 	'charset'=>'UTF-8'
			// ));
			// $api->executeCommand();
			// For PostgreSQL use:
			// $api = new PgSQL_CRUD_API(array(
			// 	'hostname'=>'localhost',
			// 	'username'=>'xxx',
			// 	'password'=>'xxx',
			// 	'database'=>'xxx',
			// 	'charset'=>'UTF8'
			// ));
			// $api->executeCommand();
			Base api = new Base (context,new Config{ });
			api.ExecuteCommand ();
		}

		public bool IsReusable {
			get {
				return false;
			}
		}
	}
}