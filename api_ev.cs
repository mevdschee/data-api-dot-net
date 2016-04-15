using System;
using System.IO;
using MySql.Data;
using System.Net;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Web;
using MySql.Data.MySqlClient;
using System.Text.RegularExpressions;
using System.Linq;
using System.Web.Script.Serialization;
using EvHttpSharp;

class WebServer {
	EventHttpMultiworkerListener _listener;
	string _host;
	int _port;

	public WebServer(string host,int port, int workers) {
		_listener = new EventHttpMultiworkerListener(RequestHandler, workers);
		_host = host;
		_port = port;
	}

	public void Start() {
		_listener.Start(_host, (ushort) _port);
	}

	private void RequestHandler(EventHttpRequest req)
	{
		ThreadPool.QueueUserWorkItem(_ =>
			{
				var headers = new Dictionary<string,string>(){{ "Content-Type","application/json; charset=utf-8" }};
				var msg = "";

				JavaScriptSerializer json = new JavaScriptSerializer ();

				// get the HTTP method, path and body of the request
				string method = req.Method;
				string[] request = req.Uri.Trim('/').Split('/');
				string data = Encoding.UTF8.GetString (req.RequestBody);
				Dictionary<string,object> input = json.Deserialize<Dictionary<string,object>>(data); 

				// connect to the sql server database
				MySqlConnection link = new MySqlConnection("addr=localhost;uid=user;pwd=pass;database=dbname");
				link.Open();

				// retrieve the table and key from the path
				string table = Regex.Replace(request[0], "[^a-z0-9_]+", "");
				int key = request.Length>1 ? int.Parse(request[1]) : 0;

				// escape the columns from the input object
				string[] columns = input!=null ? input.Keys.Select(i => Regex.Replace(i.ToString(), "[^a-z0-9_]+", "")).ToArray() : null;

				// build the SET part of the SQL command
				string set = input != null ? String.Join (", ", columns.Select (i => "[" + i + "]=@_" + i).ToArray ()) : "";

				// create SQL based on HTTP method
				string sql = null;
				switch (method) {
				case "GET":
					sql = string.Format ("select * from `{0}`" + (key > 0 ? " where `id`=@pk" : ""), table); break;
				case "PUT":
					sql = string.Format ("update `{0}` set {1} where `id`=@pk",table,set); break;
				case "POST":
					sql = string.Format ("insert into `{0}` set {1}; select last_insert_id()",table,set); break;
				case "DELETE":
					sql = string.Format ("delete `{0}` where `id`=@pk",table); break;
				}

				// add parameters to command
				MySqlCommand command = new MySqlCommand(sql, link);
				if (input!=null) foreach (string c in columns) command.Parameters.AddWithValue ("@_"+c, input[c]);
				if (key>0) command.Parameters.AddWithValue ("@pk", key);

				// print results, insert id or affected row count
				if (method == "GET") {
					MySqlDataReader reader = command.ExecuteReader ();
					var fields = new List<string> ();
					for (int i = 0; i < reader.FieldCount; i++) fields.Add (reader.GetName(i));
					if (key == 0) msg += "[";
					bool first = true;
					while (reader.Read ()) {
						if (first) first = false;
						else msg += ",";
						Dictionary<string, object> row = new Dictionary<string, object> ();
						foreach (var field in fields) row.Add (field, reader [field]);
						msg += json.Serialize ((object)row);
					}
					if (key == 0) msg += "]";
					reader.Close ();
				} else if (method == "POST") {
					MySqlDataReader reader = command.ExecuteReader ();
					reader.NextResult ();
					reader.Read ();
					msg += json.Serialize ((object)reader.GetValue (0));
					reader.Close ();
				} else {
					msg += json.Serialize ((object) command.ExecuteNonQuery ());
				}

				// close mysql connection
				link.Close ();

				req.Respond(System.Net.HttpStatusCode.OK, headers, Encoding.UTF8.GetBytes(msg));
			});
	}

	static void Main(string[] args) {
		LibLocator.Init(null);
		var w = new WebServer("127.0.0.1",8000,Environment.ProcessorCount);
		w.Start();
	}
}