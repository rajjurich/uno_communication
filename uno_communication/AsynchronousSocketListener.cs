using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using uno_communication.Common;

namespace uno_communication
{
    public class AsynchronousSocketListener
    {
        // Thread signal.  
        public ManualResetEvent allDone = new ManualResetEvent(false);
        private Logs _logs;
        private SQLHelperClass _helper;
        private string _socketType;
        private string _logType;
        private string _ip;
        string conn = GenericFunctions.Conn;

        public AsynchronousSocketListener()
        {

        }

        public void StartListening()
        {
            _logs = new Logs();
            _socketType = "Listener";
            _logType = "ErrorLog";
            _ip = "Application";
            _helper = new SQLHelperClass(_logs, _socketType, _logType, _ip);

            // Data buffer for incoming data.  
            byte[] bytes = new Byte[1024];

            // Establish the local endpoint for the socket.  
            // The DNS name of the computer  
            // running the listener is "host.contoso.com".  
            IPHostEntry ipHostInfo = Dns.Resolve(Dns.GetHostName());
            IPAddress ipAddress = ipHostInfo.AddressList[0];
            IPEndPoint localEndPoint = new IPEndPoint(ipAddress, 4366);

            // Create a TCP/IP socket.  
            Socket listener = new Socket(AddressFamily.InterNetwork,
                SocketType.Stream, ProtocolType.Tcp);

            // Bind the socket to the local endpoint and listen for incoming connections.  
            try
            {
                listener.Bind(localEndPoint);
                listener.Listen(100);
                _logs.WriteLogs("Waiting for a connection...", "Application", "Service", "AuditLog");
                while (true)
                {
                    // Set the event to nonsignaled state.  
                    allDone.Reset();

                    // Start an asynchronous socket to listen for connections.  
                    //Console.WriteLine("Waiting for a connection...");                    
                    
                    listener.BeginAccept(
                        new AsyncCallback(AcceptCallback),
                        listener);

                    // Wait until a connection is made before continuing.  
                    allDone.WaitOne();
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                _logs.ExceptionCaught(e.Message, _ip, "AsynchronousSocketListener.StartListening", "Service", "ErrorLog");
            }

            //Console.WriteLine("\nPress ENTER to continue...");
            //Console.Read();

        }

        public void AcceptCallback(IAsyncResult ar)
        {
            
            _socketType = "Listener";
            try
            {
                //_logs.WriteLogs("Connection request recieved", "Listener");
                // Signal the main thread to continue.  
                allDone.Set();

                // Get the socket that handles the client request.  
                Socket listener = (Socket)ar.AsyncState;
                Socket handler = listener.EndAccept(ar);
                // Create the state object.  
                StateObject state = new StateObject();
                state.workSocket = handler;
                handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0,
                    new AsyncCallback(ReadCallback), state);
                _ip = handler.RemoteEndPoint.ToString().Substring(0, handler.RemoteEndPoint.ToString().LastIndexOf(":"));
                _logs.WriteLogs("Connect request recieved from " + _ip, "Application", "Service", "AuditLog");
                _logs.WriteLogs("Connected to " + _ip, _ip, _socketType, "AuditLog");
                
            }
            catch (Exception e)
            {
                //Console.WriteLine(e.ToString());
                _logs.ExceptionCaught(e.Message, _ip, "AsynchronousSocketListener.AcceptCallback", _socketType, "ErrorLog");
            }
        }

        public void ReadCallback(IAsyncResult ar)
        {
            try
            {
                String content = String.Empty;

                // Retrieve the state object and the handler socket  
                // from the asynchronous state object.  
                StateObject state = (StateObject)ar.AsyncState;
                Socket handler = state.workSocket;

                // Read data from the client socket.   
                int bytesRead = handler.EndReceive(ar);

                if (bytesRead > 0)
                {
                    // There  might be more data, so store the data received so far.  
                    state.sb.Append(Encoding.ASCII.GetString(
                        state.buffer, 0, bytesRead));

                    // Check for end-of-file tag. If it is not there, read   
                    // more data.  
                    content = state.sb.ToString();
                    _logs.WriteLogs("Read " + content.Length + " bytes from " + _ip, _ip, _socketType, "AuditLog");
                    ArrayList RecData = new ArrayList();
                    RecData.AddRange(state.buffer);
                    //SqlConnection conn = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["CommunicationServer.Properties.Settings.ConnectionString"].ConnectionString);
                    _logs.WriteLogs("Packet Received " + DateTime.Now.ToString() + " from " + _ip, _ip, _socketType, "AuditLog");
                    try
                    {
                        #region CreatePacketandSaveInDatabase
                        StringBuilder sb = new StringBuilder();
                        sb.Clear();
                        sb.Append("<Packet>");
                        uint Command = ((uint)state.buffer[10] << 8) | (uint)state.buffer[11];
                        uint TotalEvents = ((uint)state.buffer[12] << 8) | (uint)state.buffer[13];
                        uint EventCount = (uint)state.buffer[14];

                        #region forloop
                        for (int i = 0; i < EventCount; i++)
                        {
                            int EventStart = (i * 32) + 15;
                            uint EventType = state.buffer[EventStart];
                            //string EventType = Encoding.ASCII.GetString(state.buffer, EventStart, 1);
                            if (EventType == 1)
                            {
                                sb.Append("<Transaction>");
                                string TerminalIp = handler.RemoteEndPoint.ToString().Substring(0, handler.RemoteEndPoint.ToString().LastIndexOf(":"));
                                uint Secs = state.buffer[EventStart + 1];
                                uint Mins = state.buffer[EventStart + 2];
                                uint Hours = state.buffer[EventStart + 3];
                                uint DayOfMonth = state.buffer[EventStart + 4];
                                uint Month = state.buffer[EventStart + 5];
                                uint Year = state.buffer[EventStart + 6] + (uint)2000;
                                uint DayOfWeek = state.buffer[EventStart + 7];
                                uint Trace = ((uint)state.buffer[EventStart + 8] << 8) | (uint)state.buffer[EventStart + 9];
                                string EmployeeNumber = Encoding.ASCII.GetString(state.buffer, EventStart + 10, 10);
                                string CardId = "";
                                for (int j = 0; j < 4; j++)
                                {
                                    CardId += state.buffer[EventStart + 20 + j].ToString("X").PadLeft(2, '0');
                                }
                                uint ReaderID = (uint)state.buffer[EventStart + 24];
                                uint TerminalId = ((uint)state.buffer[EventStart + 25] << 8) | (uint)state.buffer[EventStart + 26];
                                uint stat = state.buffer[EventStart + 27];
                                string CenterCode = state.buffer[EventStart + 28].ToString("X2");
                                uint ReaderMode = state.buffer[EventStart + 29];
                                uint AlarmType = (uint)00;
                                uint AlarmAction = (uint)00;
                                sb.Append("<EventType>" + EventType + "</EventType>");
                                sb.Append("<EventDatetime>" + new DateTime((int)Year, (int)Month, (int)DayOfMonth, (int)Hours, (int)Mins, (int)Secs) + "</EventDatetime>");
                                sb.Append("<EventDayOfWeek>" + Enum.GetName(typeof(DayOfWeek), DayOfWeek) + "</EventDayOfWeek>");
                                sb.Append("<EventTrace>" + Trace + "</EventTrace>");
                                sb.Append("<EventEmployeeCode>" + EmployeeNumber + "</EventEmployeeCode>");
                                sb.Append("<EventCardCode>" + CardId + "</EventCardCode>");
                                sb.Append("<EventReaderID>" + ReaderID + "</EventReaderID>");
                                sb.Append("<EventControllerID>" + TerminalId + "</EventControllerID>");
                                sb.Append("<EventStatus>" + stat + "</EventStatus>");
                                sb.Append("<EventAlarmType>" + AlarmType + "</EventAlarmType>");
                                sb.Append("<EventAlarmAction>" + AlarmAction + "</EventAlarmAction>");
                                sb.Append("<EventCount>" + TotalEvents + "</EventCount>");
                                sb.Append("<EventFlag>" + 0 + "</EventFlag>");
                                sb.Append("<TASCFlag>" + 0 + "</TASCFlag>");
                                sb.Append("<CenterCode>" + CenterCode + "</CenterCode>");
                                sb.Append("<ReaderMode>" + ReaderMode + "</ReaderMode>");
                                sb.Append("</Transaction>");
                            }
                            if (EventType == 2)
                            {
                                sb.Append("<Transaction>");
                                string TerminalIp = handler.RemoteEndPoint.ToString().Substring(0, handler.RemoteEndPoint.ToString().LastIndexOf(":"));
                                uint Secs = state.buffer[EventStart + 1];
                                uint Mins = state.buffer[EventStart + 2];
                                uint Hours = state.buffer[EventStart + 3];
                                uint DayOfMonth = state.buffer[EventStart + 4];
                                uint Month = state.buffer[EventStart + 5];
                                uint Year = state.buffer[EventStart + 6] + (uint)2000;
                                uint DayOfWeek = state.buffer[EventStart + 7];
                                uint Trace = ((uint)state.buffer[EventStart + 8] << 8) | (uint)state.buffer[EventStart + 9];
                                string EmployeeNumber = Encoding.ASCII.GetString(state.buffer, EventStart + 10, 10);
                                string CardId = "";
                                for (int j = 0; j < 4; j++)
                                {
                                    CardId += state.buffer[EventStart + 21 + j].ToString("X").PadLeft(2, '0');
                                }
                                uint ReaderID = (uint)state.buffer[EventStart + 24];
                                uint TerminalId = ((uint)state.buffer[EventStart + 25] << 8) | (uint)state.buffer[EventStart + 26];
                                uint stat = (uint)00;
                                uint AlarmType = ((uint)state.buffer[EventStart + 27] << 8) | (uint)state.buffer[EventStart + 28];
                                uint AlarmAction = (uint)state.buffer[EventStart + 29];
                                sb.Append("<EventType>" + EventType + "</EventType>");
                                sb.Append("<EventDatetime>" + new DateTime((int)Year, (int)Month, (int)DayOfMonth, (int)Hours, (int)Mins, (int)Secs) + "</EventDatetime>");
                                sb.Append("<EventDayOfWeek>" + Enum.GetName(typeof(DayOfWeek), DayOfWeek) + "</EventDayOfWeek>");
                                sb.Append("<EventTrace>" + Trace + "</EventTrace>");
                                sb.Append("<EventEmployeeCode>" + EmployeeNumber + "</EventEmployeeCode>");
                                sb.Append("<EventCardCode>" + CardId + "</EventCardCode>");
                                sb.Append("<EventReaderID>" + ReaderID + "</EventReaderID>");
                                sb.Append("<EventControllerID>" + TerminalId + "</EventControllerID>");
                                sb.Append("<EventStatus>" + stat + "</EventStatus>");
                                sb.Append("<EventAlarmType>" + AlarmType + "</EventAlarmType>");
                                sb.Append("<EventAlarmAction>" + AlarmAction + "</EventAlarmAction>");
                                sb.Append("<EventCount>" + TotalEvents + "</EventCount>");
                                sb.Append("<EventFlag>" + 0 + "</EventFlag>");
                                sb.Append("<TASCFlag>" + 0 + "</TASCFlag>");
                                sb.Append("<CenterCode>" + 0 + "</CenterCode>");
                                sb.Append("<ReaderMode>" + "</ReaderMode>");
                                sb.Append("</Transaction>");
                            }
                        }
                        #endregion
                        sb.Append("</Packet>");

                        string s = sb.ToString();
                        #region dbinsert
                        try
                        {
                            string query = "spInsertACS_Events";
                            List<SqlParameter> parameters = new List<SqlParameter>();
                            parameters.Add(new SqlParameter("@xmlString", sb.ToString()));
                            SqlParameter Error = new SqlParameter("@Error", SqlDbType.VarChar, 50);
                            Error.Direction = ParameterDirection.Output;
                            parameters.Add(Error);
                            _helper._ip = _ip;
                            
                            if (_helper.ExecuteProcedure(conn, query, parameters.ToArray()) != null)
                            {
                                if (Error.Value.ToString() == "0")
                                {
                                    DateTime dt = DateTime.Now;
                                    byte[] NormalResponse = new byte[29];
                                    NormalResponse[0] = Convert.ToByte(06);
                                    NormalResponse[1] = Convert.ToByte(207);
                                    NormalResponse[2] = Convert.ToByte((uint)state.buffer[4]);
                                    NormalResponse[3] = Convert.ToByte((uint)state.buffer[5]);
                                    NormalResponse[4] = Convert.ToByte((uint)state.buffer[2]);
                                    NormalResponse[5] = Convert.ToByte((uint)state.buffer[3]);
                                    NormalResponse[6] = Convert.ToByte((uint)state.buffer[6]);
                                    NormalResponse[7] = Convert.ToByte(0);
                                    NormalResponse[8] = Convert.ToByte(09);
                                    NormalResponse[9] = Convert.ToByte(01);
                                    //Data Start
                                    NormalResponse[10] = Convert.ToByte(02);
                                    NormalResponse[11] = Convert.ToByte(02);
                                    NormalResponse[12] = Convert.ToByte(00);
                                    NormalResponse[13] = Convert.ToByte(00);
                                    NormalResponse[14] = Convert.ToByte(dt.Second);
                                    NormalResponse[15] = Convert.ToByte(dt.Minute);
                                    NormalResponse[16] = Convert.ToByte(dt.Hour);
                                    NormalResponse[17] = Convert.ToByte(dt.Day);
                                    NormalResponse[18] = Convert.ToByte(dt.Month);
                                    NormalResponse[19] = Convert.ToByte(dt.Year - 2000);
                                    NormalResponse[20] = Convert.ToByte(dt.DayOfWeek);
                                    NormalResponse[21] = Convert.ToByte(00);
                                    NormalResponse[22] = Convert.ToByte(00);
                                    NormalResponse[23] = Convert.ToByte(00);
                                    NormalResponse[24] = Convert.ToByte(00);
                                    //Data End
                                    NormalResponse[25] = Convert.ToByte(07);
                                    NormalResponse[26] = Convert.ToByte(222);
                                    NormalResponse[27] = Convert.ToByte(0);
                                    NormalResponse[28] = Convert.ToByte(0);
                                    // Echo the data back to the client.
                                    _logs.WriteLogs("Sending Normal Response to " + _ip, _ip, _socketType, "AuditLog");
                                    Send(handler, NormalResponse);
                                }
                                else
                                {
                                    _logs.WriteLogs("Sending " + sb.ToString().Length + " bytes to " + _ip + " failed. ]\n" + sb.ToString() + "\n[ .........", _ip, _socketType, "PacketLog");
                                    throw new Exception("Database insert failed procedure name spInsertACS_Events check PacketLog");
                                }
                            }
                            else
                            {
                                throw new Exception("Kcronos Connection");
                            }
                        }
                        catch (Exception ex)
                        {
                            throw ex;
                        }
                        #endregion

                        #endregion
                    }
                    catch (Exception ex)
                    {
                        _logs.ExceptionCaught(ex.Message, _ip, "AsynchronousSocketListener.ReadCallback", _socketType, "ErrorLog");

                        DateTime dt = DateTime.Now;
                        byte[] ErrorResponse = new byte[29];

                        ErrorResponse[0] = Convert.ToByte(06);
                        ErrorResponse[1] = Convert.ToByte(207);
                        ErrorResponse[2] = Convert.ToByte((uint)state.buffer[4]);
                        ErrorResponse[3] = Convert.ToByte((uint)state.buffer[5]);
                        ErrorResponse[4] = Convert.ToByte((uint)state.buffer[2]);
                        ErrorResponse[5] = Convert.ToByte((uint)state.buffer[3]);
                        ErrorResponse[6] = Convert.ToByte((uint)state.buffer[6]);
                        ErrorResponse[7] = Convert.ToByte(0);
                        ErrorResponse[8] = Convert.ToByte(09);
                        ErrorResponse[9] = Convert.ToByte(01);
                        //Data Start
                        ErrorResponse[10] = Convert.ToByte(02);
                        ErrorResponse[11] = Convert.ToByte(02);
                        ErrorResponse[12] = Convert.ToByte(00);
                        ErrorResponse[13] = Convert.ToByte(01);
                        ErrorResponse[14] = Convert.ToByte(dt.Second);
                        ErrorResponse[15] = Convert.ToByte(dt.Minute);
                        ErrorResponse[16] = Convert.ToByte(dt.Hour);
                        ErrorResponse[17] = Convert.ToByte(dt.Day);
                        ErrorResponse[18] = Convert.ToByte(dt.Month);
                        ErrorResponse[19] = Convert.ToByte(dt.Year - 2000);
                        ErrorResponse[20] = Convert.ToByte(dt.DayOfWeek);
                        ErrorResponse[21] = Convert.ToByte(00);
                        ErrorResponse[22] = Convert.ToByte(00);
                        ErrorResponse[23] = Convert.ToByte(00);
                        ErrorResponse[24] = Convert.ToByte(00);
                        //Data End
                        ErrorResponse[25] = Convert.ToByte(07);
                        ErrorResponse[26] = Convert.ToByte(222);
                        ErrorResponse[27] = Convert.ToByte(0);
                        ErrorResponse[28] = Convert.ToByte(0);
                        _logs.WriteLogs("Sending Error Response to " + _ip, _ip, _socketType, "AuditLog");
                        Send(handler, ErrorResponse);

                    }
                    //Send(handler, content);
                    //if (content.IndexOf("<EOF>") > -1)
                    //{
                    //    // All the data has been read from the   
                    //    // client. Display it on the console.  
                    //    Console.WriteLine("Read {0} bytes from socket. \n Data : {1}",
                    //        content.Length, content);
                    //    // Echo the data back to the client.  
                    //    Send(handler, content);
                    //}
                    //else
                    //{
                    //    // Not all data received. Get more.  
                    //    handler.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0,
                    //    new AsyncCallback(ReadCallback), state);
                    //}
                }

            }
            catch (Exception e)
            {
                //Console.WriteLine(e.ToString());
                _logs.ExceptionCaught(e.Message, _ip, "AsynchronousSocketListener.ReadCallback", _socketType, "ErrorLog");
            }
        }

        private void Send(Socket handler, byte[] data)
        {
            try
            {
                // Convert the string data to byte data using ASCII encoding.  
                byte[] byteData = data;// Encoding.ASCII.GetBytes(data);

                // Begin sending the data to the remote device.  
                handler.BeginSend(byteData, 0, byteData.Length, 0,
                    new AsyncCallback(SendCallback), handler);
            }
            catch (Exception e)
            {
                //Console.WriteLine(e.ToString());
                _logs.ExceptionCaught(e.Message, _ip, "AsynchronousSocketListener.Send", _socketType, "ErrorLog");
            }
        }

        private void SendCallback(IAsyncResult ar)
        {
            try
            {
                // Retrieve the socket from the state object.  
                Socket handler = (Socket)ar.AsyncState;
                // Complete sending the data to the remote device.  
                int bytesSent = handler.EndSend(ar);
                //Console.WriteLine("Sent {0} bytes to client.", bytesSent);
                _logs.WriteLogs("Sent " + bytesSent + " bytes to " + _ip + ".", _ip, _socketType, "AuditLog");

                handler.Shutdown(SocketShutdown.Both);
                handler.Close();
            }
            catch (Exception e)
            {
                //Console.WriteLine(e.ToString());
                _logs.ExceptionCaught(e.Message, _ip, "AsynchronousSocketListener.SendCallback", _socketType, "ErrorLog");
            }
        }
    }
}
