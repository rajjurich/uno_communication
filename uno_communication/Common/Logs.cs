using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace uno_communication.Common
{
    public class Logs
    {
        public void WriteLogs(string message, string IP, string SocketType, string LogType)
        {
            string date = DateTime.Now.ToString("ddMMMyyyy");
            string path = GenericFunctions.AuditLogPath;
            path = path + @"\" + date + @"\" + SocketType + @"\" + LogType + @"\";

            try
            {
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }
                StringBuilder sb = new StringBuilder();
                sb.AppendLine("[ " + DateTime.Now.ToString() + " ] [ " + message + " ] ");
                using (StreamWriter writer = new StreamWriter(path + "Log_" + IP + "_" + DateTime.Now.ToString("ddMMMyyyy") + ".log", true))
                {
                    writer.Write(sb.ToString());
                    writer.Flush();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message, "AuditLog");
            }
        }

        public void ExceptionCaught(string message, string IP, string functionName, string SocketType, string LogType)
        {
            WriteLogs(functionName + " ]\r\n[ " + message, IP, SocketType, LogType);
        }
    }

}
