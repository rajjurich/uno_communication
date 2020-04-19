using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using uno_communication.Common;

namespace uno_communication
{
    public partial class Service : ServiceBase
    {
        private Timer TimerProcessing = null;
        private Object thisLock = new Object();
        string conn = GenericFunctions.Conn;
        private Logs _logs;
        private SQLHelperClass _helper;
        private string _socketType;
        private string _logType;
        private string _ip;
        private AsynchronousClient _asyncclient;
        System.Threading.Thread serverThread;
        public Service()
        {
            InitializeComponent();
        }
        public void OnDebug()
        {
            OnStart(null);
        }
        protected override void OnStart(string[] args)
        {
            AsynchronousSocketListener objListener = new AsynchronousSocketListener();
            serverThread = new System.Threading.Thread(objListener.StartListening);
            serverThread.Start();
            TimerProcessing = new Timer();
            this.TimerProcessing.Interval = Convert.ToDouble(GenericFunctions.Interval);
            this.TimerProcessing.Elapsed += new System.Timers.ElapsedEventHandler(this.TimerProcessing_Elapsed);
            TimerProcessing.Start();
        }
        protected override void OnStop()
        {
            TimerProcessing.Stop();
        }
        private void TimerProcessing_Elapsed(object sender, ElapsedEventArgs e)
        {
            Logs logs = new Logs();
            _socketType = "Service";
            _logType = "ErrorLog";
            _ip = "Application";
            SQLHelperClass helper = new SQLHelperClass(logs, _socketType, _logType, _ip);
            try
            {
                while (true)
                {
                    if (!serverThread.IsAlive)
                        serverThread.Start();
                    TimerProcessing.Stop();
                    string query = "Select * from dbo.ACS_CONTROLLER with(nolock) where CTLR_ISDELETED = 0";
                    DataTable dt = helper.ExecuteDatatable(conn, CommandType.Text, query);
                    System.Threading.Thread.Sleep(30000);
                    if (dt != null && dt.Rows.Count > 0)
                        Parallel.For(0, dt.Rows.Count, j => Process(dt, j));
                    //TimerProcessing.Start();
                }
            }
            catch (Exception ex)
            {
                logs.ExceptionCaught(ex.Message, _ip, "Service.TimerProcessing_Elapsed", _socketType, _logType);
                TimerProcessing.Start();
            }
        }
        public void Process(DataTable dt, int i)
        {
            _ip = dt.Rows[i]["CTLR_IP"].ToString();
            _socketType = "Client";

            try
            {
                System.Threading.Thread processcomm = new System.Threading.Thread(() => ProcessCommunication(dt, i));
                if (RunningThreads.List.ContainsKey(dt.Rows[i]["CTLR_IP"].ToString()))
                {
                    //    //GenericFunctions.AuditLog(dt.Rows[i]["CTLR_IP"].ToString(), " Process Skipped ");
                }
                else
                {
                    lock (thisLock)
                    {
                        if (!RunningThreads.List.ContainsKey(dt.Rows[i]["CTLR_IP"].ToString()))
                            RunningThreads.List.Add(dt.Rows[i]["CTLR_IP"].ToString(), processcomm);
                    }
                    processcomm.Start();
                }
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.Process", _socketType, "ErrorLog");
                if (RunningThreads.List.ContainsKey(dt.Rows[i]["CTLR_IP"].ToString()))
                    RunningThreads.List.Remove(dt.Rows[i]["CTLR_IP"].ToString());
            }
        }

        public void ProcessCommunication(DataTable dt, int j)
        {
            try
            {
                _logs = new Logs();


                _helper = new SQLHelperClass(_logs, _socketType, "AuditLog", _ip);
                _asyncclient = new AsynchronousClient();
                _logs.WriteLogs(" ---------- Process Communication Started ---------- ", dt.Rows[j]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                if (GetDeviceStatus(dt, j))
                {
                    if (dt.Rows[j]["CTLR_CONN_STATUS"].ToString() == "0")
                    {
                        DownloadDeviceConfiguration(dt, j);
                        ConfigureAccessLevel(dt, j);
                        ConfigureTimeZones(dt, j);
                        ConfigureHolidayList(dt, j);
                        ConfigureAccessPoint(dt, j);
                        ConfigureUserProfiles(dt, j);
                        SetDateAndTime(dt, j);
                        GetDateAndTime(dt, j);
                    }
                }
                _logs.WriteLogs(" ---------- Process Communication Ended ---------- ", dt.Rows[j]["CTLR_IP"].ToString(), _socketType, "AuditLog");
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.ProcessCommunication", _socketType, "ErrorLog");
                //TimerProcessing.Start();
            }
            lock (thisLock)
            {
                if (RunningThreads.List.ContainsKey(dt.Rows[j]["CTLR_IP"].ToString()))
                    RunningThreads.List.Remove(dt.Rows[j]["CTLR_IP"].ToString());
            }
        }

        private bool GetDeviceStatus(DataTable dt, int i)
        {
            try
            {
                #region Packet
                List<byte> lstData = new List<byte>();
                lstData.Add(06);//STX
                lstData.Add(207);//STX
                lstData.Add(00);//Source ID
                lstData.Add(01);//Source ID
                //Destination ID Start
                string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                DstID = (Convert.ToInt32(DstID)).ToString("X");
                if (DstID.Length < 4)
                {
                    for (int j = DstID.Length; j < 4; j++)
                    {
                        DstID = "0" + DstID;
                    }
                }
                lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                //Destination ID End
                lstData.Add(01);//Sequence No
                lstData.Add(06);//Length
                lstData.Add(06);//Length
                lstData.Add(01);//Application ID
                //Data Start
                //Command Start
                int Command = 257;
                lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                lstData.Add((byte)(Command & 0x00FF));//command L
                //Command End
                DateTime dtCurrent = DateTime.Now;
                lstData.Add((byte)dtCurrent.Second); //Seconds
                lstData.Add((byte)dtCurrent.Minute); //Minutes
                lstData.Add((byte)dtCurrent.Hour); // Hours
                lstData.Add((byte)dtCurrent.Day); // Day of Month
                lstData.Add((byte)dtCurrent.Month); // Month
                lstData.Add((byte)(dtCurrent.Year - 2000));//Year
                lstData.Add((byte)dtCurrent.DayOfWeek); // Day of week
                lstData.Add((byte)00);//RFU
                lstData.Add((byte)00);//RFU
                lstData.Add((byte)00);//RFU
                lstData.Add((byte)00);//RFU
                //Data End
                lstData.Add(07);//ETX
                lstData.Add(222);//ETX
                lstData.Add(00);//CRC
                lstData.Add(00);//CRC
                //Update Data Length Start
                lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                #endregion
                byte[] packet = lstData.ToArray();
                AsynchronousClient client = new AsynchronousClient();
                ReturnFlag ret = client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "0101", _logs, _helper);
                if (!ret.Flag)
                    return false;
                return true;
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.GetDeviceStatus", _socketType, "ErrorLog");
                return false;
            }
        }
        private void DownloadDeviceConfiguration(DataTable dt, int i)
        {
            try
            {
                #region Packet
                List<byte> lstData = new List<byte>();
                lstData.Add(06);//STX
                lstData.Add(207);//STX
                lstData.Add(00);//Source ID
                lstData.Add(01);//Source ID
                //Destination ID Start
                string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                DstID = (Convert.ToInt32(DstID)).ToString("X");
                if (DstID.Length < 4)
                {
                    for (int j = DstID.Length; j < 4; j++)
                    {
                        DstID = "0" + DstID;
                    }
                }

                lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                //Destination ID End
                lstData.Add(01);//Sequence No
                lstData.Add(06);//Length
                lstData.Add(06);//Length
                lstData.Add(01);//Application ID
                //Data Start
                //Command Start
                int Command = 267;
                lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                lstData.Add((byte)(Command & 0x00FF));//command L
                //Command End
                lstData.Add((byte)((dt.Rows[i]["CTLR_CHK_FACILITY_CODE"].ToString() == "Y") ? 1 : 0));//Check FC
                //FC1 - FC6 Start
                string FC1 = dt.Rows[i]["CTLR_FACILITY_CODE1"].ToString() == "" ? "0" : dt.Rows[i]["CTLR_FACILITY_CODE1"].ToString();
                lstData.Add((byte)Convert.ToInt32(FC1, 16));
                string FC2 = dt.Rows[i]["CTLR_FACILITY_CODE2"].ToString() == "" ? "0" : dt.Rows[i]["CTLR_FACILITY_CODE2"].ToString();
                lstData.Add((byte)Convert.ToInt32(FC2, 16));
                string FC3 = dt.Rows[i]["CTLR_FACILITY_CODE3"].ToString() == "" ? "0" : dt.Rows[i]["CTLR_FACILITY_CODE3"].ToString();
                lstData.Add((byte)Convert.ToInt32(FC3, 16));
                string FC4 = dt.Rows[i]["CTLR_FACILITY_CODE4"].ToString() == "" ? "0" : dt.Rows[i]["CTLR_FACILITY_CODE4"].ToString();
                lstData.Add((byte)Convert.ToInt32(FC4, 16));
                string FC5 = dt.Rows[i]["CTLR_FACILITY_CODE5"].ToString() == "" ? "0" : dt.Rows[i]["CTLR_FACILITY_CODE5"].ToString();
                lstData.Add((byte)Convert.ToInt32(FC5, 16));
                string FC6 = dt.Rows[i]["CTLR_FACILITY_CODE6"].ToString() == "" ? "0" : dt.Rows[i]["CTLR_FACILITY_CODE6"].ToString();
                lstData.Add((byte)Convert.ToInt32(FC6, 16));
                //FC1 - FC6 End
                lstData.Add((byte)((dt.Rows[i]["CTLR_CHK_APB"].ToString() == "Y") ? 1 : 0));// Local Anti-passback
                //Antipassback type Start
                string antipassbacktype = dt.Rows[i]["CTLR_APB_TYPE"].ToString();
                if (antipassbacktype == "T")
                {
                    lstData.Add(00);
                }
                else if (antipassbacktype == "M")
                {
                    lstData.Add(01);
                }
                else
                {
                    lstData.Add(02);
                }
                //Antipassback type End
                lstData.Add(30);// Communication Timeout
                lstData.Add(3);// Communication Retries
                lstData.Add(00);// RFU
                lstData.Add(00);// RFU
                lstData.Add(00);// RFU
                lstData.Add(00);// RFU
                //Data End
                lstData.Add(07);//ETX
                lstData.Add(222);//ETX
                lstData.Add(00);//CRC
                lstData.Add(00);//CRC
                //Update Data Length Start
                lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                #endregion
                byte[] packet = lstData.ToArray();
                AsynchronousClient client = new AsynchronousClient();
                client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "010B", _logs, _helper);
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.DownloadDeviceConfiguration", _socketType, "ErrorLog");
            }
        }
        private void ConfigureAccessLevel(DataTable dt, int i)
        {
            try
            {
                string query = "spCSGetAccessLevelData";
                List<SqlParameter> parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter("@ControllerID", dt.Rows[i]["CTLR_ID"].ToString()));
                DataTable dtAccessLevel = new DataTable();
                DataSet dsAccessLevel = new DataSet();
                dsAccessLevel = _helper.ExecuteProcedure(conn, query, parameters.ToArray());
                dtAccessLevel = dsAccessLevel.Tables[0];

                if (dtAccessLevel.Rows.Count != 0)
                {
                    #region Packet
                    List<byte> lstData = new List<byte>();
                    lstData.Add(06);//STX
                    lstData.Add(207);//STX
                    lstData.Add(00);//Source ID
                    lstData.Add(01);//Source ID
                    //Destination ID Start
                    string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                    DstID = (Convert.ToInt32(DstID)).ToString("X");
                    if (DstID.Length < 4)
                    {
                        for (int j = DstID.Length; j < 4; j++)
                        {
                            DstID = "0" + DstID;
                        }
                    }
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                    //Destination ID End
                    lstData.Add(01);//Sequence No
                    lstData.Add(06);//Length
                    lstData.Add(06);//Length
                    lstData.Add(01);//Application ID
                    //Data Start
                    //Command Start
                    int Command = 273;
                    lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                    lstData.Add((byte)(Command & 0x00FF));//command L
                    //Command End
                    //Access Level Start
                    for (int j = 0; j < dtAccessLevel.Rows.Count; j++)
                    {
                        lstData.Add((byte)Convert.ToInt32(dtAccessLevel.Rows[j]["AL_ID"].ToString()));// AL_ID
                        lstData.Add((byte)Convert.ToInt32(dtAccessLevel.Rows[j]["AccesLevelArray"].ToString().PadLeft(8, '0'), 2)); // AL
                        lstData.Add((byte)Convert.ToInt32(dtAccessLevel.Rows[j]["RD_ZN_ID"].ToString()));//AL_TZ
                    }
                    if (dt.Rows.Count < 128)
                    {
                        for (int j = dt.Rows.Count; j < 128; j++)
                        {
                            lstData.Add((byte)00);//AL_ID
                            lstData.Add((byte)00);//AL
                            lstData.Add((byte)00);//AL_TZ
                        }
                    }
                    //Access Level End
                    lstData.Add((byte)00); // RFU
                    lstData.Add((byte)00); // RFU
                    lstData.Add((byte)00); // RFU
                    lstData.Add((byte)00); // RFU
                    lstData.Add((byte)00); // RFU
                    lstData.Add((byte)00); // RFU
                    //Data End
                    lstData.Add(07);//ETX
                    lstData.Add(222);//ETX
                    lstData.Add(00);//CRC
                    lstData.Add(00);//CRC
                    //Update Data Length Start
                    lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                    lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                    #endregion
                    byte[] packet = lstData.ToArray();
                    AsynchronousClient client = new AsynchronousClient();
                    client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "0111", _logs, _helper, dtAccessLevel);
                }
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.ConfigureAccessLevel", _socketType, "ErrorLog");
            }
        }
        private void ConfigureTimeZones(DataTable dt, int i)
        {
            try
            {
                string query = "spCSGetTimeZoneData";
                List<SqlParameter> parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter("@ControllerID", dt.Rows[i]["CTLR_ID"].ToString()));
                DataTable dtTimeZone = new DataTable();
                DataSet dsTimeZone = new DataSet();
                dsTimeZone = _helper.ExecuteProcedure(conn, query, parameters.ToArray());
                dtTimeZone = dsTimeZone.Tables[0];
                if (dtTimeZone.Rows.Count != 0)
                {
                    #region Packet
                    List<byte> lstData = new List<byte>();
                    lstData.Add(06);//STX
                    lstData.Add(207);//STX
                    lstData.Add(00);//Source ID
                    lstData.Add(01);//Source ID
                    //Destination ID Start
                    string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                    DstID = (Convert.ToInt32(DstID)).ToString("X");
                    if (DstID.Length < 4)
                    {
                        for (int j = DstID.Length; j < 4; j++)
                        {
                            DstID = "0" + DstID;
                        }
                    }
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                    //Destination ID End
                    lstData.Add(01);//Sequence No
                    lstData.Add(06);//Length
                    lstData.Add(06);//Length
                    lstData.Add(01);//Application ID
                    //Data Start
                    //Command Start
                    int Command = 275;
                    lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                    lstData.Add((byte)(Command & 0x00FF));//command L
                    //Command End
                    DataView viewTZCode = new DataView(dtTimeZone);
                    DataTable distinctTZCode = viewTZCode.ToTable(true, "TimeZoneID");
                    lstData.Add((byte)distinctTZCode.Rows.Count);// No of Valid Time Zone
                    // Time ZOne Configuration Start
                    for (int j = 0; j < distinctTZCode.Rows.Count; j++)
                    {
                        lstData.Add((byte)Convert.ToInt32(distinctTZCode.Rows[j][0].ToString()));
                        for (int k = 1; k <= 12; k++)
                        {
                            var TimeZoneData = from row in dtTimeZone.AsEnumerable()
                                               where row.Field<string>("TimeZoneID") == distinctTZCode.Rows[j][0].ToString() &&
                                               row.Field<string>("PeriodID") == k.ToString()
                                               select row;
                            DataRow[] TimeZoneRows = TimeZoneData.ToArray();
                            if (TimeZoneRows.Length == 0)
                            {
                                lstData.Add((byte)00);// TZ_TW
                                lstData.Add((byte)00);// TZ_TW
                                lstData.Add((byte)00);// TZ_TW
                                lstData.Add((byte)00);// TZ_TW
                                lstData.Add((byte)00);// TZ_DOW
                                lstData.Add((byte)00);// TZ_DOW
                            }
                            else
                            {
                                DataTable dtTimeZoneRows = TimeZoneRows.CopyToDataTable();
                                DateTime FromDate = Convert.ToDateTime(dtTimeZoneRows.Rows[0]["FromTime"].ToString());
                                DateTime ToDate = Convert.ToDateTime(dtTimeZoneRows.Rows[0]["ToTime"].ToString());
                                string DayPattern = dtTimeZoneRows.Rows[0]["DayPattern"].ToString().PadLeft(16, '0');
                                lstData.Add((byte)FromDate.Hour);//TZ_TW
                                lstData.Add((byte)FromDate.Minute);//TZ_TW
                                lstData.Add((byte)ToDate.Hour);//TZ_TW
                                lstData.Add((byte)FromDate.Minute);//TZ_TW
                                int TZ_DOW = Convert.ToInt32(DayPattern, 2);
                                lstData.Add((byte)((TZ_DOW >> 8) & 0x00FF));//TZ_DOW H 
                                lstData.Add((byte)(TZ_DOW & 0x00FF));//TZ_DOW L
                            }
                        }
                        lstData.Add((byte)00); //RFU
                        lstData.Add((byte)00); //RFU
                        lstData.Add((byte)00); //RFU
                    }
                    // Time ZOne Configuration End
                    lstData.Add((byte)00); //RFU
                    lstData.Add((byte)00); //RFU
                    lstData.Add((byte)00); //RFU
                    lstData.Add((byte)00); //RFU
                    lstData.Add((byte)00); //RFU

                    //Data End
                    lstData.Add(07);//ETX
                    lstData.Add(222);//ETX
                    lstData.Add(00);//CRC
                    lstData.Add(00);//CRC
                    //Update Data Length Start
                    lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                    lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                    #endregion
                    byte[] packet = lstData.ToArray();
                    AsynchronousClient client = new AsynchronousClient();
                    client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "0113", _logs, _helper, dtTimeZone);
                }
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.ConfigureTimeZones", _socketType, "ErrorLog");
            }
        }

        private void ConfigureHolidayList(DataTable dt, int i)
        {
            try
            {
                string query = "Select top 64 * from Ent_holiday where HOLIDAY_ISDELETED = 0";
                DataTable dtHolidays = new DataTable();
                dtHolidays = _helper.ExecuteDatatable(conn, CommandType.Text, query);

                if (dtHolidays.Rows.Count != 0)
                {
                    #region Packet
                    List<byte> lstData = new List<byte>();
                    lstData.Add(06);//STX
                    lstData.Add(207);//STX
                    lstData.Add(00);//Source ID
                    lstData.Add(01);//Source ID
                    //Destination ID Start
                    string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                    DstID = (Convert.ToInt32(DstID)).ToString("X");
                    if (DstID.Length < 4)
                    {
                        for (int j = DstID.Length; j < 4; j++)
                        {
                            DstID = "0" + DstID;
                        }
                    }
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                    //Destination ID End
                    lstData.Add(01);//Sequence No
                    lstData.Add(06);//Length
                    lstData.Add(06);//Length
                    lstData.Add(01);//Application ID
                    //Data Start
                    //Command Start
                    int Command = 277;
                    lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                    lstData.Add((byte)(Command & 0x00FF));//command L
                    //Command End
                    lstData.Add((byte)dtHolidays.Rows.Count);//No of valid Holidays
                    for (int j = 0; j < dtHolidays.Rows.Count; j++)
                    {
                        lstData.Add((byte)01);//valid : 1 = Valid, 0 : invalid
                        DateTime HolidayDate = Convert.ToDateTime(dtHolidays.Rows[j]["HOLIDAY_DATE"].ToString());
                        lstData.Add((byte)HolidayDate.Day);//Date
                        lstData.Add((byte)HolidayDate.Month);//Month
                        lstData.Add((byte)00);//RFU
                    }
                    lstData.Add((byte)00);//RFU
                    lstData.Add((byte)00);//RFU
                    lstData.Add((byte)00);//RFU
                    lstData.Add((byte)00);//RFU
                    lstData.Add((byte)00);//RFU
                    //Data End
                    lstData.Add(07);//ETX
                    lstData.Add(222);//ETX
                    lstData.Add(00);//CRC
                    lstData.Add(00);//CRC
                    //Update Data Length Start
                    lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                    lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                    #endregion
                    byte[] packet = lstData.ToArray();
                    AsynchronousClient client = new AsynchronousClient();
                    client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "0115", _logs, _helper);
                }
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.ConfigureHolidayList", _socketType, "ErrorLog");
            }
        }

        private void ConfigureAccessPoint(DataTable dt, int i)
        {
            try
            {
                string query = "spCSGetAccessPointData";
                List<SqlParameter> parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter("@ControllerID", dt.Rows[i]["CTLR_ID"].ToString()));
                DataTable dtAccessPoint = new DataTable();
                DataSet dsAccessPoint = new DataSet();
                dsAccessPoint = _helper.ExecuteProcedure(conn, query, parameters.ToArray());
                dtAccessPoint = dsAccessPoint.Tables[0];
                bool ret = false;
                if (dtAccessPoint.Rows.Count != 0)
                {
                    Console.WriteLine(ret);
                    #region Packet
                    List<byte> lstData = new List<byte>();
                    lstData.Add(06);//STX
                    lstData.Add(207);//STX
                    lstData.Add(00);//Source ID
                    lstData.Add(01);//Source ID
                    //Destination ID Start
                    string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                    DstID = (Convert.ToInt32(DstID)).ToString("X");
                    if (DstID.Length < 4)
                    {
                        for (int j = DstID.Length; j < 4; j++)
                        {
                            DstID = "0" + DstID;
                        }
                    }
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                    //Destination ID End
                    lstData.Add(01);//Sequence No
                    lstData.Add(06);//Length
                    lstData.Add(06);//Length
                    lstData.Add(01);//Application ID
                    //Data Start
                    //Command Start
                    int Command = 279;
                    lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                    lstData.Add((byte)(Command & 0x00FF));//command L
                    //Command End
                    //Access Point Start 
                    for (int j = 0; j < dtAccessPoint.Rows.Count; j++)
                    {
                        lstData.Add((byte)00);//Access Point Status
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["AccessPointType"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["AccessPointType"].ToString()));// Access Point Type
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["Reader1"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["Reader1"].ToString())); // Reader 1
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["Reader2"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["Reader2"].ToString())); // Reader 2
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["Door1"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["Door1"].ToString())); // Door 1
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["Door2"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["Door2"].ToString())); // Door 2
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["DoorOpenTime"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["DoorOpenTime"].ToString())); // Door Open Time
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["EntryReaderCardFormat"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["EntryReaderCardFormat"].ToString())); // Entry Reader Card Format
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["EntryReaderMode"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["EntryReaderMode"].ToString())); // Entry Reader Mode
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["ExitReaderCardFormat"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["ExitReaderCardFormat"].ToString())); // Exit Reader card Format
                        lstData.Add((byte)Convert.ToInt32(dtAccessPoint.Rows[j]["ExitReaderMode"].ToString() == "" ? "0" : dtAccessPoint.Rows[j]["ExitReaderMode"].ToString())); // Exit Reader Mode
                        lstData.Add((byte)00); // Door Monitored
                        lstData.Add((byte)00); // RFU
                        lstData.Add((byte)00); // RFU
                        lstData.Add((byte)00); // RFU
                        lstData.Add((byte)00); // RFU
                    }
                    for (int j = dtAccessPoint.Rows.Count; j < 8; j++)
                    {
                        lstData.Add((byte)00);//Access Point Status
                        lstData.Add((byte)00);// Access Point Type
                        lstData.Add((byte)00); // Reader 1
                        lstData.Add((byte)00); // Reader 2
                        lstData.Add((byte)00); // Door 1
                        lstData.Add((byte)00); // Door 2
                        lstData.Add((byte)00); // Door Open Time
                        lstData.Add((byte)00); // Entry Reader Card Format
                        lstData.Add((byte)00); // Entry Reader Mode
                        lstData.Add((byte)00); // Exit Reader card Format
                        lstData.Add((byte)00); // Exit Reader Mode
                        lstData.Add((byte)00); // Door Monitored
                        lstData.Add((byte)00); // RFU
                        lstData.Add((byte)00); // RFU
                        lstData.Add((byte)00); // RFU
                        lstData.Add((byte)00); // RFU
                    }
                    //Access Point End
                    lstData.Add((byte)00); // RFU
                    lstData.Add((byte)00); // RFU
                    lstData.Add((byte)00); // RFU
                    lstData.Add((byte)00); // RFU
                    //Data End
                    lstData.Add(07);//ETX
                    lstData.Add(222);//ETX
                    lstData.Add(00);//CRC
                    lstData.Add(00);//CRC
                    //Update Data Length Start
                    lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                    lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                    #endregion
                    byte[] packet = lstData.ToArray();
                    AsynchronousClient client = new AsynchronousClient();
                    client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "0117", _logs, _helper);
                }
                query = "Update ACS_ACCESSPOINT_RELATION set AP_FLAG = -1 where AP_CONTROLLER_ID = " + dt.Rows[i]["CTLR_ID"].ToString();
                _helper.ExecuteNonQuery(conn, CommandType.Text, query);
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.ConfigureAccessPoint", _socketType, "ErrorLog");
            }
        }

        private void ConfigureUserProfiles(DataTable dt, int i)
        {
            try
            {
                int num1 = 0;
                //logs.WriteLogs("Configure User Profile Started.", dt.Rows[i]["CTLR_IP"].ToString());
                string query = "spCSGetProfilesData";
                List<SqlParameter> parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter("@ControllerID", dt.Rows[i]["CTLR_ID"].ToString()));
                DataTable dtUserProfiles = new DataTable();
                DataSet dsUserProfiles = new DataSet();
                dsUserProfiles = _helper.ExecuteProcedure(conn, query, parameters.ToArray());
                dtUserProfiles = dsUserProfiles.Tables[0];

                #region ADDPROFILEs
                //logs.WriteLogs("Configure User Add Profile Started.", dt.Rows[i]["CTLR_IP"].ToString());
                DataTable dtAddProfiles = new DataTable();
                var rowsAdd = from row in dtUserProfiles.AsEnumerable() where row.Field<string>("ProfileAction").Trim() == "0" select row;
                DataRow[] rowsArrayAdd = rowsAdd.ToArray();
                if (rowsArrayAdd.Length != 0)
                {
                    dtAddProfiles = rowsAdd.CopyToDataTable();
                    for (int x = 0; x < dtAddProfiles.Rows.Count; x++)
                    {
                        try
                        {
                            #region Packet
                            List<byte> lstData = new List<byte>();
                            lstData.Add(06);//STX
                            lstData.Add(207);//STX
                            lstData.Add(00);//Source ID
                            lstData.Add(01);//Source ID
                            //Destination ID Start
                            string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                            DstID = (Convert.ToInt32(DstID)).ToString("X");
                            if (DstID.Length < 4)
                            {
                                for (int j = DstID.Length; j < 4; j++)
                                {
                                    DstID = "0" + DstID;
                                }
                            }
                            lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                            lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                            //Destination ID End
                            lstData.Add(01);//Sequence No
                            lstData.Add(06);//Length
                            lstData.Add(06);//Length
                            lstData.Add(01);//Application ID
                            //Data Start
                            //Command Start
                            int Command = 285;
                            lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                            lstData.Add((byte)(Command & 0x00FF));//command L
                            //Command End
                            //No OF Profiles Start
                            int NoOfProfiles = 1;
                            lstData.Add((byte)NoOfProfiles);//No. Of Profiles
                            // No OF Profiles End
                            lstData.Add((byte)x);//Packet Sequence Counter
                            lstData.Add((byte)00);//Profile Action

                            //Card ID Start
                            string cardCode = dtAddProfiles.Rows[x]["CardCode"].ToString().PadLeft(8, '0');
                            //Logger.Log("StartClient : " + cardCode);
                            for (int k = 0; k < 8; k += 2)
                            {
                                lstData.Add((byte)Convert.ToInt32((cardCode.Substring(k, 2)), 16));
                            }
                            //Card ID End
                            //Issue Date Start
                            DateTime StartDate = Convert.ToDateTime(dtAddProfiles.Rows[x]["ActivationDate"]);
                            lstData.Add((byte)StartDate.Day);
                            lstData.Add((byte)StartDate.Month);
                            lstData.Add((byte)Convert.ToInt32(StartDate.Year.ToString().Substring(2, 2)));
                            //Issue Date End
                            //Expire Date Start
                            DateTime ExpiryDate = Convert.ToDateTime(dtAddProfiles.Rows[x]["ExpiryDate"]);
                            lstData.Add((byte)ExpiryDate.Day);
                            lstData.Add((byte)ExpiryDate.Month);
                            lstData.Add((byte)Convert.ToInt32(ExpiryDate.Year.ToString().Substring(2, 2)));
                            //Expiry Date End
                            //PIN Start
                            string PIN = dtAddProfiles.Rows[x]["PIN"].ToString();
                            if (PIN != "")
                            {
                                if (PIN.Length < 6)
                                {
                                    for (int len = PIN.Length; len < 6; len++)
                                    {
                                        PIN += "0";
                                    }
                                }

                                byte[] array1 = new byte[6];
                                array1 = Encoding.ASCII.GetBytes(PIN);

                                for (int k = 0; k < array1.Length; k++)
                                {
                                    array1[k] = Convert.ToByte((int)array1[k] - 48);
                                }

                                for (int q = 0; q < array1.Length; q += 2)
                                {
                                    lstData.Add((byte)((array1[q] << 4) | (array1[q + 1] & 0x00FF)));
                                }
                            }
                            else
                            {
                                for (int P = 0; P < 3; P++)
                                {
                                    lstData.Add(0);
                                }
                            }
                            //PIN End
                            //Employee No. Start
                            string EmployeeCode = dtAddProfiles.Rows[x]["EmployeeCode"].ToString();
                            EmployeeCode = EmployeeCode.PadLeft(10, '0');
                            //EmployeeCode = "00" + EmployeeCode.Substring(0, 2) + "0" + EmployeeCode.Substring(2);
                            for (int k = 0; k < EmployeeCode.Length; k++)
                            {
                                lstData.Add((byte)Convert.ToInt32(EmployeeCode[k]));
                            }
                            //Employee No. End
                            //Access Level Array Start
                            string ALArray = dtAddProfiles.Rows[x]["ALArray"].ToString();
                            if (ALArray == "")
                            {
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                            }
                            else
                            {
                                ALArray = ALArray.Remove(0, 1);
                                string[] arrALArray = ALArray.Split(',');
                                if (arrALArray.Length > 4)
                                {
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                }
                                else
                                {
                                    for (int k = 0; k < arrALArray.Length; k++)
                                    {
                                        lstData.Add((byte)Convert.ToInt32(arrALArray[k].ToString(), 2));
                                    }
                                    for (int k = arrALArray.Length; k < 4; k++)
                                    {
                                        lstData.Add((byte)00);
                                    }
                                }
                            }
                            //Access Level Array End
                            //Special Function Start
                            lstData.Add(00);
                            //Special Function End
                            //Access Mode Start
                            if (dtAddProfiles.Rows[x]["AuthMode"].ToString() == "C")
                            {
                                lstData.Add(00);
                            }
                            else if (dtAddProfiles.Rows[x]["AuthMode"].ToString() == "F")
                            {
                                lstData.Add(06);
                            }
                            else if (dtAddProfiles.Rows[x]["AuthMode"].ToString() == "CF")
                            {
                                lstData.Add(03);
                            }
                            else
                            {
                                lstData.Add(00);
                            }
                            //Access Mode End
                            //APB Status Start
                            lstData.Add(00);
                            //APB Status End
                            //RFU Start
                            lstData.Add(00);
                            lstData.Add(00);
                            //RFU End
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            //Data End
                            lstData.Add(07);//ETX
                            lstData.Add(222);//ETX
                            lstData.Add(00);//CRC
                            lstData.Add(00);//CRC
                            //Update Data Length Start
                            lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                            lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                            #endregion

                            byte[] packet = lstData.ToArray();
                            //ClientSocket obj = new ClientSocket();
                            AsynchronousClient client = new AsynchronousClient();
                            ReturnFlag ret = client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "011D", _logs, _helper, dtAddProfiles);
                            if (ret.Flag)
                            {
                                string queryUpdate = "UPDATE EAL_CONFIG SET FLAG = -1 WHERE EAL_ID = '" + dtAddProfiles.Rows[x]["EALID"].ToString() + "'";
                                _helper.ExecuteNonQuery(conn, CommandType.Text, queryUpdate);
                                _logs.WriteLogs("Add Profile " + dtAddProfiles.Rows[x]["EmployeeCode"].ToString() + " Success. ", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Added Successfully for " + dtAddProfiles.Rows[x]["EmployeeCode"].ToString(), "Successful", "0", _helper);
                            }
                            else
                            {
                                _logs.WriteLogs("Add Profile " + dtAddProfiles.Rows[x]["EmployeeCode"].ToString() + " failed check Error Log. Retrying...", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Addition Failed for " + dtAddProfiles.Rows[x]["EmployeeCode"].ToString() + " due to " + ret.Error + ". Retrying...", "Failed", "0", _helper);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logs.WriteLogs("Add Profile " + dtAddProfiles.Rows[x]["EmployeeCode"].ToString() + " failed due to " + ex.Message + ". Retrying...", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                            _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Addition Failed for " + dtAddProfiles.Rows[x]["EmployeeCode"].ToString() + " due to " + ex.Message + ". Retrying...", "Failed", "0", _helper);
                        }
                    }
                }
                #endregion


                #region UPDATEPROFILEs
                //logs.WriteLogs("Configure User Update Profile Started.", dt.Rows[i]["CTLR_IP"].ToString());
                DataTable dtUpdateProfiles = new DataTable();
                var rowsEdit = from row in dtUserProfiles.AsEnumerable() where row.Field<string>("ProfileAction").Trim() == "1" select row;
                DataRow[] rowsArrayEdit = rowsEdit.ToArray();
                if (rowsArrayEdit.Length != 0)
                {
                    dtUpdateProfiles = rowsEdit.CopyToDataTable();

                    for (int x = 0; x < dtUpdateProfiles.Rows.Count; x++)
                    {
                        try
                        {
                            #region Packet
                            List<byte> lstData = new List<byte>();
                            lstData.Add(06);//STX
                            lstData.Add(207);//STX
                            lstData.Add(00);//Source ID
                            lstData.Add(01);//Source ID
                            //Destination ID Start
                            string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                            DstID = (Convert.ToInt32(DstID)).ToString("X");
                            if (DstID.Length < 4)
                            {
                                for (int j = DstID.Length; j < 4; j++)
                                {
                                    DstID = "0" + DstID;
                                }
                            }
                            lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                            lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                            //Destination ID End
                            lstData.Add(01);//Sequence No
                            lstData.Add(06);//Length
                            lstData.Add(06);//Length
                            lstData.Add(01);//Application ID
                            //Data Start
                            //Command Start
                            int Command = 285;
                            lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                            lstData.Add((byte)(Command & 0x00FF));//command L
                            //Command End
                            //No Of Profiles Start
                            int NoOfProfiles = 1;
                            lstData.Add((byte)NoOfProfiles);//No. Of Profiles
                            // No Of Profiles End
                            lstData.Add((byte)x);//Packet Sequence Counter
                            lstData.Add((byte)01);//Profile Action

                            //Card ID Start
                            string cardCode = dtUpdateProfiles.Rows[x]["CardCode"].ToString().PadLeft(8, '0');
                            //Logger.Log("StartClient : " + cardCode);
                            for (int k = 0; k < 8; k += 2)
                            {
                                lstData.Add((byte)Convert.ToInt32((cardCode.Substring(k, 2)), 16));
                            }
                            //Card ID End
                            //Issue Date Start
                            DateTime StartDate = Convert.ToDateTime(dtUpdateProfiles.Rows[x]["ActivationDate"]);
                            lstData.Add((byte)StartDate.Day);
                            lstData.Add((byte)StartDate.Month);
                            lstData.Add((byte)Convert.ToInt32(StartDate.Year.ToString().Substring(2, 2)));
                            //Issue Date End
                            //Expire Date Start
                            DateTime ExpiryDate = Convert.ToDateTime(dtUpdateProfiles.Rows[x]["ExpiryDate"]);
                            lstData.Add((byte)ExpiryDate.Day);
                            lstData.Add((byte)ExpiryDate.Month);
                            lstData.Add((byte)Convert.ToInt32(ExpiryDate.Year.ToString().Substring(2, 2)));
                            //Expiry Date End
                            //PIN Start
                            string PIN = dtUpdateProfiles.Rows[x]["PIN"].ToString();
                            if (PIN != "")
                            {
                                if (PIN.Length < 6)
                                {
                                    for (int len = PIN.Length; len < 6; len++)
                                    {
                                        PIN += "0";
                                    }
                                }

                                byte[] array1 = new byte[6];
                                array1 = Encoding.ASCII.GetBytes(PIN);

                                for (int k = 0; k < array1.Length; k++)
                                {
                                    array1[k] = Convert.ToByte((int)array1[k] - 48);
                                }

                                for (int q = 0; q < array1.Length; q += 2)
                                {
                                    lstData.Add((byte)((array1[q] << 4) | (array1[q + 1] & 0x00FF)));
                                }
                            }
                            else
                            {
                                for (int P = 0; P < 3; P++)
                                {
                                    lstData.Add(0);
                                }
                            }
                            //PIN End
                            //Employee No. Start
                            string EmployeeCode = dtUpdateProfiles.Rows[x]["EmployeeCode"].ToString();
                            EmployeeCode = EmployeeCode.PadLeft(10, '0');
                            //EmployeeCode = "00" + EmployeeCode.Substring(0, 2) + "0" + EmployeeCode.Substring(2);
                            for (int k = 0; k < EmployeeCode.Length; k++)
                            {
                                lstData.Add((byte)Convert.ToInt32(EmployeeCode[k]));
                            }
                            //Employee No. End
                            //Access Level Array Start
                            string ALArray = dtUpdateProfiles.Rows[x]["ALArray"].ToString();
                            if (ALArray == "")
                            {
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                            }
                            else
                            {
                                ALArray = ALArray.Remove(0, 1);
                                string[] arrALArray = ALArray.Split(',');
                                if (arrALArray.Length > 4)
                                {
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                }
                                else
                                {
                                    for (int k = 0; k < arrALArray.Length; k++)
                                    {
                                        lstData.Add((byte)Convert.ToInt32(arrALArray[k].ToString(), 2));
                                    }
                                    for (int k = arrALArray.Length; k < 4; k++)
                                    {
                                        lstData.Add((byte)00);
                                    }
                                }
                            }
                            //Access Level Array End
                            //Special Function Start
                            lstData.Add(00);
                            //Special Function End
                            //Access Mode Start
                            if (dtUpdateProfiles.Rows[x]["AuthMode"].ToString() == "C")
                            {
                                lstData.Add(00);
                            }
                            else if (dtUpdateProfiles.Rows[x]["AuthMode"].ToString() == "F")
                            {
                                lstData.Add(06);
                            }
                            else if (dtUpdateProfiles.Rows[x]["AuthMode"].ToString() == "CF")
                            {
                                lstData.Add(03);
                            }
                            else
                            {
                                lstData.Add(00);
                            }
                            //Access Mode End
                            //APB Status Start
                            lstData.Add(00);
                            //APB Status End
                            //RFU Start
                            lstData.Add(00);
                            lstData.Add(00);
                            //RFU End

                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            //Data End
                            lstData.Add(07);//ETX
                            lstData.Add(222);//ETX
                            lstData.Add(00);//CRC
                            lstData.Add(00);//CRC
                            //Update Data Length Start
                            lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                            lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                            #endregion
                            byte[] packet = lstData.ToArray();
                            AsynchronousClient client = new AsynchronousClient();
                            ReturnFlag ret = client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "011D", _logs, _helper, dtUpdateProfiles);
                            if (ret.Flag)
                            {
                                string queryUpdate = "UPDATE EAL_CONFIG SET FLAG = -1 WHERE EAL_ID = '" + dtUpdateProfiles.Rows[x]["EALID"].ToString() + "'";
                                _helper.ExecuteNonQuery(conn, CommandType.Text, queryUpdate);
                                _logs.WriteLogs("Update Profile " + dtUpdateProfiles.Rows[x]["EmployeeCode"].ToString() + " Success. ", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Update Successfully for " + dtUpdateProfiles.Rows[x]["EmployeeCode"].ToString(), "Successful", "0", _helper);
                            }
                            else
                            {
                                _logs.WriteLogs("Update Profile " + dtUpdateProfiles.Rows[x]["EmployeeCode"].ToString() + " failed check Error Log. Retrying...", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Update Failed for " + dtUpdateProfiles.Rows[x]["EmployeeCode"].ToString() + " due to " + ret.Error + ". Retrying...", "Failed", "0", _helper);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logs.WriteLogs("Update Profile " + dtUpdateProfiles.Rows[x]["EmployeeCode"].ToString() + " failed due to " + ex.Message + ". Retrying...", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                            _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Update Failed for " + dtUpdateProfiles.Rows[x]["EmployeeCode"].ToString() + " due to " + ex.Message + ". Retrying...", "Failed", "0", _helper);
                        }

                    }

                }
                //logs.WriteLogs("Configure User Update Profile Ended.", dt.Rows[i]["CTLR_IP"].ToString());
                #endregion


                #region DELETEPROFILEs
                //logs.WriteLogs("Configure User Delete Profile Started.", dt.Rows[i]["CTLR_IP"].ToString());
                DataTable dtDeleteProfiles = new DataTable();
                var rowsDelete = from row in dtUserProfiles.AsEnumerable() where row.Field<string>("ProfileAction").Trim() == "2" select row;
                DataRow[] rowsArrayDelete = rowsDelete.ToArray();
                if (rowsArrayDelete.Length != 0)
                {
                    dtDeleteProfiles = rowsDelete.CopyToDataTable();

                    for (int x = 0; x < dtDeleteProfiles.Rows.Count; x++)
                    {
                        try
                        {
                            #region Packet
                            List<byte> lstData = new List<byte>();
                            lstData.Add(06);//STX
                            lstData.Add(207);//STX
                            lstData.Add(00);//Source ID
                            lstData.Add(01);//Source ID
                            //Destination ID Start
                            string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                            DstID = (Convert.ToInt32(DstID)).ToString("X");
                            if (DstID.Length < 4)
                            {
                                for (int j = DstID.Length; j < 4; j++)
                                {
                                    DstID = "0" + DstID;
                                }
                            }
                            lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                            lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                            //Destination ID End
                            lstData.Add(01);//Sequence No
                            lstData.Add(06);//Length
                            lstData.Add(06);//Length
                            lstData.Add(01);//Application ID
                            //Data Start
                            //Command Start
                            int Command = 285;
                            lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                            lstData.Add((byte)(Command & 0x00FF));//command L
                            //Command End
                            //No Of Profiles Start
                            int NoOfProfiles = 1;
                            lstData.Add((byte)NoOfProfiles);//No. Of Profiles
                            // No Of Profiles End
                            lstData.Add((byte)x);//Packet Sequence Counter
                            lstData.Add((byte)02);//Profile Action

                            //Card ID Start
                            string cardCode = dtDeleteProfiles.Rows[x]["CardCode"].ToString().PadLeft(8, '0');
                            //Logger.Log("StartClient : " + cardCode);
                            for (int k = 0; k < 8; k += 2)
                            {
                                lstData.Add((byte)Convert.ToInt32((cardCode.Substring(k, 2)), 16));
                            }
                            //Card ID End
                            //Issue Date Start
                            DateTime StartDate = Convert.ToDateTime(dtDeleteProfiles.Rows[x]["ActivationDate"]);
                            lstData.Add((byte)StartDate.Day);
                            lstData.Add((byte)StartDate.Month);
                            lstData.Add((byte)Convert.ToInt32(StartDate.Year.ToString().Substring(2, 2)));
                            //Issue Date End
                            //Expire Date Start
                            DateTime ExpiryDate = Convert.ToDateTime(dtDeleteProfiles.Rows[x]["ExpiryDate"]);
                            lstData.Add((byte)ExpiryDate.Day);
                            lstData.Add((byte)ExpiryDate.Month);
                            lstData.Add((byte)Convert.ToInt32(ExpiryDate.Year.ToString().Substring(2, 2)));
                            //Expiry Date End
                            //PIN Start
                            string PIN = dtDeleteProfiles.Rows[x]["PIN"].ToString();
                            if (PIN != "")
                            {
                                if (PIN.Length < 6)
                                {
                                    for (int len = PIN.Length; len < 6; len++)
                                    {
                                        PIN += "0";
                                    }
                                }

                                byte[] array1 = new byte[6];
                                array1 = Encoding.ASCII.GetBytes(PIN);

                                for (int k = 0; k < array1.Length; k++)
                                {
                                    array1[k] = Convert.ToByte((int)array1[k] - 48);
                                }

                                for (int q = 0; q < array1.Length; q += 2)
                                {
                                    lstData.Add((byte)((array1[q] << 4) | (array1[q + 1] & 0x00FF)));
                                }
                            }
                            else
                            {
                                for (int P = 0; P < 3; P++)
                                {
                                    lstData.Add(0);
                                }
                            }
                            //PIN End
                            //Employee No. Start
                            string EmployeeCode = dtDeleteProfiles.Rows[x]["EmployeeCode"].ToString();
                            EmployeeCode = EmployeeCode.PadLeft(10, '0');
                            //EmployeeCode = "00" + EmployeeCode.Substring(0, 2) + "0" + EmployeeCode.Substring(2);
                            for (int k = 0; k < EmployeeCode.Length; k++)
                            {
                                lstData.Add((byte)Convert.ToInt32(EmployeeCode[k]));
                            }
                            //Employee No. End
                            //Access Level Array Start
                            string ALArray = dtDeleteProfiles.Rows[x]["ALArray"].ToString();
                            if (ALArray == "")
                            {
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                                lstData.Add((byte)00);
                            }
                            else
                            {
                                ALArray = ALArray.Remove(0, 1);
                                string[] arrALArray = ALArray.Split(',');
                                if (arrALArray.Length > 4)
                                {
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                    lstData.Add((byte)00);
                                }
                                else
                                {
                                    for (int k = 0; k < arrALArray.Length; k++)
                                    {
                                        lstData.Add((byte)Convert.ToInt32(arrALArray[k].ToString(), 2));
                                    }
                                    for (int k = arrALArray.Length; k < 4; k++)
                                    {
                                        lstData.Add((byte)00);
                                    }
                                }
                            }
                            //Access Level Array End
                            //Special Function Start
                            lstData.Add(00);
                            //Special Function End
                            //Access Mode Start
                            if (dtDeleteProfiles.Rows[x]["AuthMode"].ToString() == "C")
                            {
                                lstData.Add(00);
                            }
                            else if (dtDeleteProfiles.Rows[x]["AuthMode"].ToString() == "F")
                            {
                                lstData.Add(06);
                            }
                            else if (dtDeleteProfiles.Rows[x]["AuthMode"].ToString() == "CF")
                            {
                                lstData.Add(03);
                            }
                            else
                            {
                                lstData.Add(00);
                            }
                            //Access Mode End
                            //APB Status Start
                            lstData.Add(00);
                            //APB Status End
                            //RFU Start
                            lstData.Add(00);
                            lstData.Add(00);
                            //RFU End

                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            lstData.Add(00);//RFU
                            //Data End
                            lstData.Add(07);//ETX
                            lstData.Add(222);//ETX
                            lstData.Add(00);//CRC
                            lstData.Add(00);//CRC
                            //Update Data Length Start
                            lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                            lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                            #endregion

                            byte[] packet = lstData.ToArray();
                            AsynchronousClient client = new AsynchronousClient();
                            ReturnFlag ret = client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "011D", _logs, _helper, dtDeleteProfiles);

                            if (ret.Flag)
                            {
                                string queryUpdate = "UPDATE EAL_CONFIG SET FLAG = -1 WHERE EAL_ID = '" + dtDeleteProfiles.Rows[x]["EALID"].ToString() + "'";
                                _helper.ExecuteNonQuery(conn, CommandType.Text, queryUpdate);
                                _logs.WriteLogs("Delete Profile " + dtDeleteProfiles.Rows[x]["EmployeeCode"].ToString() + " Success. ", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Deleted Successfully for " + dtDeleteProfiles.Rows[x]["EmployeeCode"].ToString(), "Successful", "0", _helper);
                            }
                            else
                            {
                                _logs.WriteLogs("Delete Profile " + dtDeleteProfiles.Rows[x]["EmployeeCode"].ToString() + " failed check Exception Log. Retrying...", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Deletion Failed for " + dtDeleteProfiles.Rows[x]["EmployeeCode"].ToString() + " due to " + ret.Error + ". Retrying...", "Failed", "0", _helper);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logs.WriteLogs("Delete Profile " + dtDeleteProfiles.Rows[x]["EmployeeCode"].ToString() + " failed due to " + ex.Message + ". Retrying...", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                            _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Deletion Failed for " + dtDeleteProfiles.Rows[x]["EmployeeCode"].ToString() + " due to " + ex.Message + ". Retrying...", "Failed", "0", _helper);
                        }
                    }
                }
                //logs.WriteLogs("Configure User Deleted Profile Started.", dt.Rows[i]["CTLR_IP"].ToString());
                #endregion


                #region INSERTPROFILEs
                DataTable dtInsertProfiles = new DataTable();
                var rowsInsert = from row in dtUserProfiles.AsEnumerable() where row.Field<string>("ProfileAction").Trim() == "3" select row;
                DataRow[] rowsArrayInsert = rowsInsert.ToArray();
                if (rowsArrayInsert.Length != 0)
                {

                    dtInsertProfiles = rowsInsert.CopyToDataTable();
                    // Delete All Start
                    #region Packet
                    List<byte> lstData = new List<byte>();
                    lstData.Add(06);//STX
                    lstData.Add(207);//STX
                    lstData.Add(00);//Source ID
                    lstData.Add(01);//Source ID
                    //Destination ID Start
                    string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                    DstID = (Convert.ToInt32(DstID)).ToString("X");
                    if (DstID.Length < 4)
                    {
                        for (int j = DstID.Length; j < 4; j++)
                        {
                            DstID = "0" + DstID;
                        }
                    }
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                    lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                    //Destination ID End
                    lstData.Add(01);//Sequence No
                    lstData.Add(06);//Length
                    lstData.Add(06);//Length
                    lstData.Add(01);//Application ID
                    //Data Start
                    //Command Start
                    int Command = 285;
                    lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                    lstData.Add((byte)(Command & 0x00FF));//command L
                    //Command End
                    lstData.Add((byte)01);//No. Of Profiles
                    lstData.Add((byte)00);//Packet Sequence Counter
                    lstData.Add((byte)09);//Profile Action

                    //Card ID Start
                    string cardCode = "00000000";
                    //Logger.Log("StartClient : " + cardCode);
                    for (int k = 0; k < 8; k += 2)
                    {
                        lstData.Add((byte)Convert.ToInt32((cardCode.Substring(k, 2)), 16));
                    }
                    //Card ID End
                    //Issue Date Start
                    lstData.Add((byte)00);
                    lstData.Add((byte)00);
                    lstData.Add((byte)00);
                    //Issue Date End
                    //Expire Date Start
                    lstData.Add((byte)00);
                    lstData.Add((byte)00);
                    lstData.Add((byte)00);
                    //Expiry Date End
                    //PIN Start
                    string PIN = "000000";
                    if (PIN != "")
                    {
                        if (PIN.Length < 6)
                        {
                            for (int len = PIN.Length; len < 6; len++)
                            {
                                PIN += "0";
                            }
                        }

                        byte[] array1 = new byte[6];
                        array1 = Encoding.ASCII.GetBytes(PIN);

                        for (int k = 0; k < array1.Length; k++)
                        {
                            array1[k] = Convert.ToByte((int)array1[k] - 48);
                        }

                        for (int q = 0; q < array1.Length; q += 2)
                        {
                            lstData.Add((byte)((array1[q] << 4) | (array1[q + 1] & 0x00FF)));
                        }
                    }
                    else
                    {
                        for (int P = 0; P < 3; P++)
                        {
                            lstData.Add(0);
                        }
                    }
                    //PIN End
                    //Employee No. Start
                    string EmployeeCode = "0000000000";
                    EmployeeCode = EmployeeCode.PadLeft(10, '0');
                    //EmployeeCode = "00" + EmployeeCode.Substring(0, 2) + "0" + EmployeeCode.Substring(2);
                    for (int k = 0; k < EmployeeCode.Length; k++)
                    {
                        lstData.Add((byte)Convert.ToInt32(EmployeeCode[k]));
                    }
                    //Employee No. End
                    //Access Level Array Start
                    string ALArray = "";
                    if (ALArray == "")
                    {
                        lstData.Add((byte)00);
                        lstData.Add((byte)00);
                        lstData.Add((byte)00);
                        lstData.Add((byte)00);
                    }
                    else
                    {
                        ALArray = ALArray.Remove(0, 1);
                        string[] arrALArray = ALArray.Split(',');
                        if (arrALArray.Length > 4)
                        {
                            lstData.Add((byte)00);
                            lstData.Add((byte)00);
                            lstData.Add((byte)00);
                            lstData.Add((byte)00);
                        }
                        else
                        {
                            for (int k = 0; k < arrALArray.Length; k++)
                            {
                                lstData.Add((byte)Convert.ToInt32(arrALArray[k].ToString(), 10));
                            }
                            for (int k = arrALArray.Length; k < 4; k++)
                            {
                                lstData.Add((byte)00);
                            }
                        }
                    }
                    //Access Level Array End
                    //Special Function Start
                    lstData.Add(00);
                    //Special Function End
                    //Access Mode Start
                    lstData.Add(00);
                    //Access Mode End
                    //APB Status Start
                    lstData.Add(00);
                    //APB Status End
                    //RFU Start
                    lstData.Add(00);
                    lstData.Add(00);
                    //RFU End

                    lstData.Add(00);//RFU
                    lstData.Add(00);//RFU
                    lstData.Add(00);//RFU
                    lstData.Add(00);//RFU
                    lstData.Add(00);//RFU
                    //Data End
                    lstData.Add(07);//ETX
                    lstData.Add(222);//ETX
                    lstData.Add(00);//CRC
                    lstData.Add(00);//CRC
                    //Update Data Length Start
                    lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                    lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                    #endregion
                    byte[] packet = lstData.ToArray();
                    AsynchronousClient client = new AsynchronousClient();
                    client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "011D", _logs, _helper);
                    System.Threading.Thread.Sleep(15000);
                    // Delete All End
                    int PacketCount = (dtInsertProfiles.Rows.Count % 32 == 0) ? (dtInsertProfiles.Rows.Count / 32) : ((dtInsertProfiles.Rows.Count / 32) + 1);
                    ReturnFlag ret = new ReturnFlag();
                    int PacketVal = 1;
                    int maxPacketVal = dtInsertProfiles.Rows.Count;
                    for (int x = 0; x < PacketCount; x++)
                    {
                        _logs.WriteLogs("sending packet " + (x + 1).ToString() + " out of " + PacketCount + " packets for insert command - attempt " + (num1 + 1).ToString() + " (Total Records : " + dtInsertProfiles.Rows.Count + ")", _ip, "Client", "AuditLog");
                        PacketVal = x * 32;
                        maxPacketVal = x == PacketCount - 1 ? dtInsertProfiles.Rows.Count : (x + 1) * 32;
                        #region Packet
                        List<byte> lstData1 = new List<byte>();
                        lstData1.Add(06);//STX
                        lstData1.Add(207);//STX
                        lstData1.Add(00);//Source ID
                        lstData1.Add(01);//Source ID
                        //Destination ID Start
                        string DstID1 = dt.Rows[i]["CTLR_ID"].ToString();
                        DstID1 = (Convert.ToInt32(DstID1)).ToString("X");
                        if (DstID1.Length < 4)
                        {
                            for (int j = DstID1.Length; j < 4; j++)
                            {
                                DstID1 = "0" + DstID1;
                            }
                        }
                        lstData1.Add((byte)Convert.ToInt32(DstID1.Substring(0, 2), 16));//Destination ID
                        lstData1.Add((byte)Convert.ToInt32(DstID1.Substring(2), 16));//Destination ID
                        //Destination ID End
                        lstData1.Add(01);//Sequence No
                        lstData1.Add(06);//Length
                        lstData1.Add(06);//Length
                        lstData1.Add(01);//Application ID
                        //Data Start
                        //Command Start
                        int Command1 = 285;
                        lstData1.Add((byte)((Command1 >> 8) & 0x00FF));//command H 
                        lstData1.Add((byte)(Command1 & 0x00FF));//command L
                        //Command End
                        //No. of Profiles Start
                        int NoOfProfiles = (x == PacketCount - 1) ? (dtInsertProfiles.Rows.Count - x * 32) : (32);
                        lstData1.Add((byte)NoOfProfiles);//No. Of Profiles
                        // No. Of Profiles End
                        int z = x + 1;
                        lstData1.Add((byte)z);//Packet Sequence Counter
                        lstData1.Add((byte)03);//Profile Action
                        for (int j = 0; j < NoOfProfiles; j++)
                        {
                            try
                            {
                                //Card ID Start
                                string cardCode1 = dtInsertProfiles.Rows[x * 32 + j]["CardCode"].ToString().PadLeft(8, '0');
                                //Logger.Log("StartClient : " + cardCode);
                                for (int k = 0; k < 8; k += 2)
                                {
                                    lstData1.Add((byte)Convert.ToInt32((cardCode1.Substring(k, 2)), 16));
                                }
                                //Card ID End
                                //Issue Date Start
                                DateTime StartDate = Convert.ToDateTime(dtInsertProfiles.Rows[x * 32 + j]["ActivationDate"]);
                                lstData1.Add((byte)StartDate.Day);
                                lstData1.Add((byte)StartDate.Month);
                                lstData1.Add((byte)Convert.ToInt32(StartDate.Year.ToString().Substring(2, 2)));
                                //Issue Date End
                                //Expire Date Start
                                DateTime ExpiryDate = Convert.ToDateTime(dtInsertProfiles.Rows[x * 32 + j]["ExpiryDate"]);
                                lstData1.Add((byte)ExpiryDate.Day);
                                lstData1.Add((byte)ExpiryDate.Month);
                                lstData1.Add((byte)Convert.ToInt32(ExpiryDate.Year.ToString().Substring(2, 2)));
                                //Expiry Date End
                                //PIN Start
                                string PIN1 = dtInsertProfiles.Rows[x * 32 + j]["PIN"].ToString();
                                if (PIN1 != "")
                                {
                                    if (PIN1.Length < 6)
                                    {
                                        for (int len = PIN1.Length; len < 6; len++)
                                        {
                                            PIN1 += "0";
                                        }
                                    }

                                    byte[] array1 = new byte[6];
                                    array1 = Encoding.ASCII.GetBytes(PIN1);

                                    for (int k = 0; k < array1.Length; k++)
                                    {
                                        array1[k] = Convert.ToByte((int)array1[k] - 48);
                                    }

                                    for (int q = 0; q < array1.Length; q += 2)
                                    {
                                        lstData1.Add((byte)((array1[q] << 4) | (array1[q + 1] & 0x00FF)));
                                    }
                                }
                                else
                                {
                                    for (int P = 0; P < 3; P++)
                                    {
                                        lstData1.Add(0);
                                    }
                                }
                                //PIN End
                                //Employee No. Start
                                string EmployeeCode1 = dtInsertProfiles.Rows[x * 32 + j]["EmployeeCode"].ToString();
                                EmployeeCode1 = EmployeeCode1.PadLeft(10, '0');
                                //EmployeeCode1 = "00" + EmployeeCode1.Substring(0, 2) + "0" + EmployeeCode1.Substring(2);
                                for (int k = 0; k < EmployeeCode1.Length; k++)
                                {
                                    lstData1.Add((byte)Convert.ToInt32(EmployeeCode1[k]));
                                }
                                //Employee No. End
                                //Access Level Array Start
                                string ALArray1 = dtInsertProfiles.Rows[x * 32 + j]["ALArray"].ToString();
                                if (ALArray1 == "")
                                {
                                    lstData1.Add((byte)00);
                                    lstData1.Add((byte)00);
                                    lstData1.Add((byte)00);
                                    lstData1.Add((byte)00);
                                }
                                else
                                {
                                    ALArray1 = ALArray1.Remove(0, 1);
                                    string[] arrALArray1 = ALArray1.Split(',');
                                    if (arrALArray1.Length > 4)
                                    {
                                        lstData1.Add((byte)00);
                                        lstData1.Add((byte)00);
                                        lstData1.Add((byte)00);
                                        lstData1.Add((byte)00);
                                    }
                                    else
                                    {
                                        for (int k = 0; k < arrALArray1.Length; k++)
                                        {
                                            lstData1.Add((byte)Convert.ToInt32(arrALArray1[k].ToString(), 10));
                                        }
                                        for (int k = arrALArray1.Length; k < 4; k++)
                                        {
                                            lstData1.Add((byte)00);
                                        }
                                    }
                                }
                                //Access Level Array End
                                //Special Function Start
                                lstData1.Add(00);
                                //Special Function End
                                //Access Mode Start
                                if (dtInsertProfiles.Rows[x * 32 + j]["AuthMode"].ToString() == "C")
                                {
                                    lstData1.Add(00);
                                }
                                else if (dtInsertProfiles.Rows[x * 32 + j]["AuthMode"].ToString() == "F")
                                {
                                    lstData1.Add(06);
                                }
                                else if (dtInsertProfiles.Rows[x * 32 + j]["AuthMode"].ToString() == "CF")
                                {
                                    lstData1.Add(03);
                                }
                                else
                                {
                                    lstData1.Add(00);
                                }
                                //Access Mode End
                                //APB Status Start
                                lstData1.Add(00);
                                //APB Status End
                                //RFU Start
                                lstData1.Add(00);
                                lstData1.Add(00);
                                //RFU End
                            }
                            catch (Exception ex)
                            {
                                _logs.WriteLogs("Insert Profile " + dtInsertProfiles.Rows[j]["EmployeeCode"].ToString() + " failed due to " + ex.Message + ". Retrying...", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Insertion Failed for " + dtInsertProfiles.Rows[j]["EmployeeCode"].ToString() + " due to " + ex.Message + ". Retrying...", "Failed", "0", _helper);
                            }
                        }
                        lstData1.Add(00);//RFU
                        lstData1.Add(00);//RFU
                        lstData1.Add(00);//RFU
                        lstData1.Add(00);//RFU
                        lstData1.Add(00);//RFU
                        //Data End
                        lstData1.Add(07);//ETX
                        lstData1.Add(222);//ETX
                        lstData1.Add(00);//CRC
                        lstData1.Add(00);//CRC
                        //Update Data Length Start
                        lstData1[7] = Convert.ToByte(((lstData1.Count - 14 >> 8) & 0x00FF)); // H Length
                        lstData1[8] = Convert.ToByte((lstData1.Count - 14 & 0x00FF)); // L Length
                        #endregion
                        byte[] packet1 = lstData1.ToArray();
                        AsynchronousClient client1 = new AsynchronousClient();
                        ReturnFlag ret1 = client1.StartClient(packet1, dt.Rows[i]["CTLR_IP"].ToString(), "011D", _logs, _helper, dtInsertProfiles);
                        System.Threading.Thread.Sleep(2000);
                        if (!ret1.Flag)
                        {
                            x--;
                            num1++;
                        }
                        else
                        {
                            num1 = 0;
                        }
                        if (num1 > 9)
                        {
                            return;
                        }
                        ret = ret1;
                        for (int y = PacketVal; y < maxPacketVal; y++)
                        {
                            if (ret1.Flag)
                            {
                                _logs.WriteLogs("Insert Profile " + dtInsertProfiles.Rows[y]["EmployeeCode"].ToString() + " Success. ", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Inserted Successfully for " + dtInsertProfiles.Rows[y]["EmployeeCode"].ToString(), "Successful", "0", _helper);
                            }
                            else
                            {
                                _logs.WriteLogs("Insert Profile " + dtInsertProfiles.Rows[y]["EmployeeCode"].ToString() + " failed check Error Log. Retrying...", dt.Rows[i]["CTLR_IP"].ToString(), _socketType, "AuditLog");
                                _asyncclient.InsertEventLogs(dt.Rows[i]["CTLR_IP"].ToString(), "Profile Insertion Failed for " + dtInsertProfiles.Rows[y]["EmployeeCode"].ToString() + " due to " + ret1.Error + ". Retrying...", "Failed", "0", _helper);
                            }
                            //else
                            //logs.WriteLogs("Insert Profile " + dtInsertProfiles.Rows[y]["EmployeeCode"].ToString() + " Failed. ", dt.Rows[i]["CTLR_IP"].ToString());
                        }


                    }
                    if (ret.Flag)
                    {

                        try
                        {
                            _logs.WriteLogs("Updating Database...", _ip, "Client", "AuditLog");
                            for (int y = 0; y < dtInsertProfiles.Rows.Count; y++)
                            {
                                string queryUpdate = "UPDATE EAL_CONFIG SET FLAG = -1 WHERE EAL_ID = '" + dtInsertProfiles.Rows[y]["EALID"].ToString() + "'";
                                _helper.ExecuteNonQuery(conn, CommandType.Text, queryUpdate);
                                //string queryUpdate = "UPDATE EAL_CONFIG SET FLAG = -1 WHERE EAL_ID = '" + dtInsertProfiles.Rows[y]["EALID"].ToString() + "'";
                                //helper.ExecuteNonQuery(conn, CommandType.Text, query);
                            }
                            _logs.WriteLogs("Database Updated.", _ip, "Client", "AuditLog");
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                    }

                }
                #endregion

                //logs.WriteLogs("Configure User Profile Ended.", dt.Rows[i]["CTLR_IP"].ToString());
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.ConfigureUserProfiles", _socketType, "ErrorLog");
            }
        }
        private void SetDateAndTime(DataTable dt, int i)
        {
            try
            {
                //logs.WriteLogs("Set Date and Time Started.", dt.Rows[i]["CTLR_IP"].ToString());
                #region Packet
                List<byte> lstData = new List<byte>();
                lstData.Add(06);//STX
                lstData.Add(207);//STX
                lstData.Add(00);//Source ID
                lstData.Add(01);//Source ID
                //Destination ID Start
                string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                DstID = (Convert.ToInt32(DstID)).ToString("X");
                if (DstID.Length < 4)
                {
                    for (int j = DstID.Length; j < 4; j++)
                    {
                        DstID = "0" + DstID;
                    }
                }
                lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                //Destination ID End
                lstData.Add(01);//Sequence No
                lstData.Add(06);//Length
                lstData.Add(06);//Length
                lstData.Add(01);//Application ID
                //Data Start
                //Command Start
                int Command = 259;
                lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                lstData.Add((byte)(Command & 0x00FF));//command L
                //Command End
                DateTime dtCurrent = DateTime.Now;
                lstData.Add((byte)dtCurrent.Second); //Seconds
                lstData.Add((byte)dtCurrent.Minute); //Minutes
                lstData.Add((byte)dtCurrent.Hour); // Hours
                lstData.Add((byte)dtCurrent.Day); // Day of Month
                lstData.Add((byte)dtCurrent.Month); // Month
                lstData.Add((byte)(dtCurrent.Year - 2000));//Year
                lstData.Add((byte)dtCurrent.DayOfWeek); // Day of week
                lstData.Add((byte)00);//RFU
                lstData.Add((byte)00);//RFU
                lstData.Add((byte)00);//RFU
                //Data End
                lstData.Add(07);//ETX
                lstData.Add(222);//ETX
                lstData.Add(00);//CRC
                lstData.Add(00);//CRC
                //Update Data Length Start
                lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                #endregion
                byte[] packet = lstData.ToArray();
                AsynchronousClient client = new AsynchronousClient();
                client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "0103", _logs, _helper);
                //logs.WriteLogs("Set Date and Time Ended.", dt.Rows[i]["CTLR_IP"].ToString());
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.SetDateAndTime", _socketType, "ErrorLog");
            }
        }
        private void GetDateAndTime(DataTable dt, int i)
        {
            try
            {
                #region Packet
                List<byte> lstData = new List<byte>();
                lstData.Add(06);//STX
                lstData.Add(207);//STX
                lstData.Add(00);//Source ID
                lstData.Add(01);//Source ID
                //Destination ID Start
                string DstID = dt.Rows[i]["CTLR_ID"].ToString();
                DstID = (Convert.ToInt32(DstID)).ToString("X");
                if (DstID.Length < 4)
                {
                    for (int j = DstID.Length; j < 4; j++)
                    {
                        DstID = "0" + DstID;
                    }
                }
                lstData.Add((byte)Convert.ToInt32(DstID.Substring(0, 2), 16));//Destination ID
                lstData.Add((byte)Convert.ToInt32(DstID.Substring(2), 16));//Destination ID
                //Destination ID End
                lstData.Add(01);//Sequence No
                lstData.Add(06);//Length
                lstData.Add(06);//Length
                lstData.Add(01);//Application ID
                //Data Start
                //Command Start
                int Command = 261;
                lstData.Add((byte)((Command >> 8) & 0x00FF));//command H 
                lstData.Add((byte)(Command & 0x00FF));//command L
                //Command End
                lstData.Add((byte)00);//RFU
                lstData.Add((byte)00);//RFU
                lstData.Add((byte)00);//RFU
                lstData.Add((byte)00);//RFU
                //Data End
                lstData.Add(07);//ETX
                lstData.Add(222);//ETX
                lstData.Add(00);//CRC
                lstData.Add(00);//CRC
                //Update Data Length Start
                lstData[7] = Convert.ToByte(((lstData.Count - 14 >> 8) & 0x00FF)); // H Length
                lstData[8] = Convert.ToByte((lstData.Count - 14 & 0x00FF)); // L Length
                #endregion
                byte[] packet = lstData.ToArray();
                AsynchronousClient client = new AsynchronousClient();
                client.StartClient(packet, dt.Rows[i]["CTLR_IP"].ToString(), "0105", _logs, _helper);
            }
            catch (Exception ex)
            {
                _logs.ExceptionCaught(ex.Message, _ip, "Service.GetDateAndTime", _socketType, "ErrorLog");
            }
        }
    }
}
