using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace uno_communication
{
    public static class GenericFunctions
    {

        public static string ImageFilepath { get; set; }
        public static string ExcelPassword { get; set; }
        public static string AuditLogPath { get; set; }
        public static string ErrotLogPath { get; set; }
        public static string ConnString { get; set; }
        public static string StandardReports { get; set; }

        public static string OutputPath { get; set; }

        public static string OutputFileName { get; set; }

        public static string TempFolder { get; set; }

        public static string SMTPHostName { get; set; }
        public static string PortNumber { get; set; }
        public static string UserName { get; set; }
        public static string Password { get; set; }
        public static string From { get; set; }

        public static string ConnStringTime { get; set; }
        public static string WaitingTime { get; set; }
        public static bool processFlag { get; set; }
        public static string ExcelType { get; set; }
        public static int CommandTimeOut { get; set; }

        public static int EmailLogicType { get; set; }

        public static Hashtable htQuery = new Hashtable();

        public static Hashtable htFileType = new Hashtable();
        public static int IsRollover { get; set; }
        public static string StandardManualReports { get; set; }
        public static string OutputFolderWeb { get; set; }
        public static string Conn { get; set; }
        public static string Interval { get; set; }

        //public static DataTable EmailRepositortDT { get; set; }
        //public static DataTable SMSRepositortDT { get; set; }
        static GenericFunctions()
        {

            ImageFilepath = Convert.ToString(ConfigurationManager.AppSettings["ImageFilepath"]);
            ExcelPassword = Convert.ToString(ConfigurationManager.AppSettings["ExcelPassword"]);
            AuditLogPath = Convert.ToString(ConfigurationManager.AppSettings["AuditLogPath"]);
            ErrotLogPath = Convert.ToString(ConfigurationManager.AppSettings["ErrotLogPath"]);
            ConnString = Convert.ToString(ConfigurationManager.AppSettings["ConnString"]);
            Conn = Convert.ToString(ConfigurationManager.AppSettings["SqlConnectionString"]);
            Interval = Convert.ToString(ConfigurationManager.AppSettings["interval"]);
            StandardReports = Convert.ToString(ConfigurationManager.AppSettings["StandardReports"]);
            SMTPHostName = Convert.ToString(ConfigurationManager.AppSettings["SMTPHostName"]);
            PortNumber = Convert.ToString(ConfigurationManager.AppSettings["PortNumber"]);
            UserName = Convert.ToString(ConfigurationManager.AppSettings["UserName"]);
            Password = Convert.ToString(ConfigurationManager.AppSettings["Password"]);
            From = Convert.ToString(ConfigurationManager.AppSettings["From"]);
            ConnStringTime = Convert.ToString(ConfigurationManager.AppSettings["ConnStringTime"]);
            WaitingTime = Convert.ToString(ConfigurationManager.AppSettings["WaitingTime"]);
            OutputPath = Convert.ToString(ConfigurationManager.AppSettings["OutputFolder"]);
            TempFolder = Convert.ToString(ConfigurationManager.AppSettings["TempFolder"]);
            ExcelType = Convert.ToString(ConfigurationManager.AppSettings["ExcelType"]);
            CommandTimeOut = Convert.ToInt32(ConfigurationManager.AppSettings["CommandTimeOut"]);
            EmailLogicType = Convert.ToInt32(ConfigurationManager.AppSettings["EmailLogicType"]);
            IsRollover = Convert.ToInt32(ConfigurationManager.AppSettings["IsRollover"]); // 1- Yes , 0 - No
            StandardManualReports = Convert.ToString(ConfigurationManager.AppSettings["StandardManualReports"]);
            OutputFolderWeb = Convert.ToString(ConfigurationManager.AppSettings["OutputFolderWeb"]);

        }


        public static void AuditLog(string strMessage, string IP)
        {
            StreamWriter SW = null;
            string date = DateTime.Now.ToString("ddMMMyyyy");
            string path = AuditLogPath;
            path = path + @"\" + date + @"\";
            try
            {
                CreateDirectory(path);
                SW = new System.IO.StreamWriter(path + "Log_" + IP + "_" + DateTime.Now.ToString("ddMMMyyyy") + ".log", true);
                SW.WriteLine("[ " + DateTime.Now.ToString() + " ] [ " + strMessage + " ] ");
                SW.Flush();
                SW.Close();
                //Thread.Sleep(10000);
            }
            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace, "AuditLog");
            }
            finally
            {
                //SW.Close();
                //SW.Dispose();
            }
        }

        public static void ExceptionLog(string message, string FunctionName)
        {
            string date = DateTime.Now.ToString("ddMMMyyyy");
            string path = ErrotLogPath;
            path = path + @"\\" + date + @"\";
            try
            {
                CreateDirectory(path);

                DateTime now = DateTime.Now;
                using (StreamWriter SW = new StreamWriter(path + "ErrorLog_" + DateTime.Now.ToString("ddMMMyyyy") + ".log", true))
                {
                    SW.WriteLine("[ " + DateTime.Now.ToString() + " ] Function Name = [ " + FunctionName + " ] " + message);
                    SW.Close();
                }

            }
            catch (Exception ex)
            {
                // ExceptionLog(ex.Message + ex.StackTrace, "ExceptionLog");
                Console.WriteLine(ex.Message);
            }
        }

        public static void CreateDirectory(string Path)
        {
            //string date = DateTime.Now.ToString("ddMMMyyyy");
            //Path = Path + @"\" + date + @"\";
            try
            {
                if (!Directory.Exists(Path))
                {
                    Directory.CreateDirectory(Path);
                }
            }
            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace + "Folder Path=" + Path, "CreateDirectory");
            }

        }

        public static void IsFileExist(string Strpath)
        {

            try
            {
                if (File.Exists(Strpath))
                {
                    File.Delete(Strpath);
                }
            }
            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace + "FilePath=" + Strpath, "IsFileExist");
            }



        }

        public static Boolean IsDirExist(string Strpath)
        {

            try
            {
                if (Directory.Exists(Strpath))
                    return true;

                else
                    return false;
            }
            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace + "FilePath=" + Strpath, "IsDirExist");
                return false;
            }



        }

        public static void FileCopy(string fileSource, string fileDestination)
        {

            try
            {
                if (File.Exists(fileDestination))
                {
                    File.Delete(fileDestination);
                }
                File.Copy(fileSource, fileDestination);
            }
            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace + "SourceFilePath=" + fileSource + "Destination=" + fileDestination, "IsFileExist");
            }



        }

        public static string EndsWithSlash(string path)
        {
            try
            {
                if (path.EndsWith("\\"))
                {
                    return path;
                }
                else
                {
                    return path + "\\";
                }

            }
            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace + " Source Path " + path, "EndsWithSlash");
                return path;

            }
        }

        public static bool Execute_Batch_file(string StrBatchFilePath)
        {
            bool isExecuteBatchFile = false;
            Process batchExecute = new Process();
            ProcessStartInfo batchExecuteInfo = new ProcessStartInfo(StrBatchFilePath);

            try
            {

                batchExecuteInfo.WindowStyle = ProcessWindowStyle.Minimized;
                batchExecuteInfo.UseShellExecute = true;
                batchExecuteInfo.CreateNoWindow = false;
                batchExecute.StartInfo = batchExecuteInfo;
                batchExecute.Start();
                //batchExecute.WaitForExit(2000);
                batchExecute.WaitForExit(10000);

                isExecuteBatchFile = true;
            }

            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace, "Execute_Batch_file");
            }
            return isExecuteBatchFile;
        }


        ////[System.Runtime.InteropServices.DllImport("user32.dll")]
        ////static extern int GetWindowThreadProcessId(int hWnd, out int lpdwProcessId);

        ////public static Process GetExcelProcess(Microsoft.Office.Interop.Excel.Application excelApp)
        ////{
        ////    try
        ////    {
        ////        int id;
        ////        GetWindowThreadProcessId(excelApp.Hwnd, out id);
        ////        return Process.GetProcessById(id);
        ////    }
        ////    catch (Exception ex)
        ////    {
        ////        return null;
        ////    }
        ////}

        public static DataTable GetDataTable_ExcelSheet(string fileName, string sheetName, string Filter)
        {
            System.Data.OleDb.OleDbConnection conn = null;
            DataTable dataResult = new DataTable();
            DataTable DtWithoutblank = new DataTable();

            try
            {
                //conn = New System.Data.OleDb.OleDbConnection("Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + fileName + ";Extended Properties=Excel 8.0;")
                conn = new System.Data.OleDb.OleDbConnection(openConn_String_XL(fileName));
                conn.Open();
                System.Data.OleDb.OleDbCommand command = new System.Data.OleDb.OleDbCommand(" SELECT * FROM [" + sheetName + "$]");
                command.Connection = conn;
                System.Data.OleDb.OleDbDataAdapter adaperForExcelBook = new System.Data.OleDb.OleDbDataAdapter();
                adaperForExcelBook.SelectCommand = command;
                adaperForExcelBook.Fill(dataResult);
                conn.Close();
                adaperForExcelBook.Dispose();
                DtWithoutblank = dataResult.Copy();

            }
            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace + "SourceFilePath=" + fileName, "GetDataTable_Excel");
            }
            finally
            {
                dataResult.Dispose();
                conn.Dispose();
            }
            return DtWithoutblank;
        }

        public static string openConn_String_XL(string sFileName)
        {
            string strConn = string.Empty;
            try
            {
                strConn = "Provider = Microsoft.Jet.OLEDB.4.0;Data Source=" + sFileName + ";Extended Properties='Excel 8.0;IMEX=1;HDR=No';";
            }

            catch (Exception ex)
            {
                ExceptionLog(ex.Message + ex.StackTrace, "openConn_String_XL");

            }
            finally
            {
            }
            return strConn;
        }

    }
}
