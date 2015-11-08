<%@ WebHandler Language="C#" Class="DataApiDotNet.Handler" %>﻿using System;
using System.Text;
using System.Web;
using System.Collections.Generic;
using System.Web.Script.Serialization;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Linq;

namespace DataApiDotNet
{
	public class Handler: System.Web.IHttpHandler
	{

		public void ProcessRequest (HttpContext context)
		{
			JavaScriptSerializer json = new JavaScriptSerializer ();

			// get the HTTP method, path and body of the request
			string method = context.Request.HttpMethod;
			string[] request = context.Request.PathInfo.Trim('/').Split('/');
			string data = Encoding.UTF8.GetString (context.Request.BinaryRead (context.Request.TotalBytes));
			Dictionary<string,object> input = json.Deserialize<Dictionary<string,object>>(data); 

			// connect to the sql server database
			SqlConnection link = new SqlConnection("addr=localhost;uid=user;pwd=pass;database=dbname");
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
				sql = string.Format ("select * from [{0}]" + (key > 0 ? " where [id]=@pk" : ""), table); break;
			case "PUT":
				sql = string.Format ("update [{0}] set {1} where [id]=@pk",table,set); break;
			case "POST":
				sql = string.Format ("insert into [{0}] set {1}; select scope_identity()",table,set); break;
			case "DELETE":
				sql = string.Format ("delete [{0}] where [id]=@pk",table); break;
			}

			// add parameters to command
			SqlCommand command = new SqlCommand(sql, link);
			if (input!=null) foreach (string c in columns) command.Parameters.AddWithValue ("@_"+c, input[c]);
			if (key>0) command.Parameters.AddWithValue ("@pk", key);

			// print results, insert id or affected row count
			if (method == "GET") {
				SqlDataReader reader = command.ExecuteReader ();
				var fields = new List<string> ();
				for (int i = 0; i < reader.FieldCount; i++) fields.Add (reader.GetName(i));
				if (key == 0) context.Response.Write ("[");
				bool first = true;
				while (reader.Read ()) {
					if (first) first = false;
					else context.Response.Write (",");
					Dictionary<string, object> row = new Dictionary<string, object> ();
					foreach (var field in fields) row.Add (field, reader [field]);
					context.Response.Write (json.Serialize ((object)row));
				}
				if (key == 0) context.Response.Write ("]");
				reader.Close ();
			} else if (method == "POST") {
				SqlDataReader reader = command.ExecuteReader ();
				reader.NextResult ();
				reader.Read ();
				context.Response.Write (json.Serialize ((object)reader.GetValue (0)));
				reader.Close ();
			} else {
				context.Response.Write (json.Serialize ((object) command.ExecuteNonQuery ()));
			}

			// close mysql connection
			link.Close ();
		}

		public bool IsReusable {
			get {
				return false;
			}
		}
	}
}