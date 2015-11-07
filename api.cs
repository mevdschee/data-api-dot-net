using System;
using System.Web;

namespace testaspx
{

	public class api : System.Web.IHttpHandler
	{

		public void ProcessRequest (HttpContext context)
		{
			context.Response.Write ("test");
		}

		public bool IsReusable {
			get {
				return false;
			}
		}
	}
}