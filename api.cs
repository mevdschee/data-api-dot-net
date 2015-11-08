using System;
using System.Text;
using System.Web;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataApiDotNet
{
	public class Handler: System.Web.IHttpHandler
	{

		public void ProcessRequest (HttpContext context)
		{
			// get the HTTP method, path and body of the request
			string method = context.Request.HttpMethod;
			string[] request = context.Request.PathInfo.Trim('/').Split('/');
			string data = Encoding.UTF8.GetString (context.Request.BinaryRead (context.Request.TotalBytes));
			Dictionary<string,object> input = JsonConvert.DeserializeObject<Dictionary<string,object>>(data); 

			// connect to the sql server database
			SqlConnection link = new SqlConnection("addr=localhost;uid=user;pwd=pass;database=dbname");

			// retrieve the table and key from the path
			string table = Regex.Replace(request[0], "[^a-z0-9_]+", "");
			int key = request.Length>1 ? int.Parse(request[1]) : 0;

			// escape the columns and values from the input object
			string[] columns = input!=null ? input.Keys.Select(i => Regex.Replace(i.ToString(), "[^a-z0-9_]+", "")).ToArray() : null;
			string[] values = input!=null ? input.Values.Select(i => i.ToString()).ToArray() : null;

			// build the SET part of the SQL command
			string set = input != null ? String.Join (",", columns.Select (i => "`" + i + "`=?").ToArray ()) : "";

			// create SQL based on HTTP method
			string sql = null;
			switch (method) {
			case "GET":
				sql = string.Format ("select * from `{0}`" + (key > 0 ? " WHERE id=?" : ""), table); break;
			case "PUT":
				sql = string.Format ("update `{0}` set {1} where id=?",table,set); break;
			case "POST":
				sql = string.Format ("insert into `{0}` set {1}",table,set); break;
			case "DELETE":
				sql = string.Format ("delete `{0}` where id=?",table); break;
			}

			/*
			context.Response.Write (set);

			// excecute SQL statement
			$result = mysqli_query($link,$sql);

			// die if SQL statement failed
			if (!$result) {
				http_response_code(404);
				die(mysqli_error());
			}

			// print results, insert id or affected row count
			if ($method == 'GET') {
				if (!$key) echo '[';
				for ($i=0;$i<mysqli_num_rows($result);$i++) {
					echo ($i>0?',':'').json_encode(mysqli_fetch_object($result));
				}
				if (!$key) echo ']';
			} elseif ($method == 'POST') {
				echo mysqli_insert_id($link);
			} else {
				echo mysqli_affected_rows($link);
			}

			// close mysql connection
			mysqli_close($link);*/
		}

		public bool IsReusable {
			get {
				return false;
			}
		}
	}
}