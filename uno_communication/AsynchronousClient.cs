using System;
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
using UnoCommunicationService;

namespace uno_communication
{
    public class AsynchronousClient
    {

        private string _ip = string.Empty;
        private Logs _logs;
        private SQLHelperClass _helper;
        private string _command = string.Empty;
        string conn = GenericFunctions.Conn;
        private Object thisLock = new Object();        
        // The port number for the remote device.  
        private const int port = 4365;

        // ManualResetEvent instances signal completion.  
        private ManualResetEvent connectDone =
            new ManualResetEvent(false);
        private ManualResetEvent sendDone =
            new ManualResetEvent(false);
        private ManualResetEvent receiveDone =
            new ManualResetEvent(false);

        // The response from the remote device.  
        private String response = String.Empty;
        private bool ConnectFlag = true;
        private bool SendFlag = true;
        private bool ReceivedFlag = true;
        private string EventLog = "0";
        public string Error = string.Empty;
        private ReturnFlag returnFlag;

        public ReturnFlag StartClient(byte[] packet, string Ip, string command, Logs logs, SQLHelperClass helper, DataTable dtProfiles = null)
        {
            _logs = logs;
            _helper = helper;
            _ip = Ip;
            _command = command;
            
            returnFlag = new ReturnFlag();
            // Connect to a remote device.  
            try
            {
                // Establish the remote endpoint for the socket.  
                // The name of the   
                // remote device is "host.contoso.com". 
                _logs.WriteLogs("Connecting to " + _ip + " for '" + command + "'.", _ip,"Client","AuditLog");
                IPHostEntry ipHostInfo = Dns.Resolve(Ip);
                IPAddress ipAddress = ipHostInfo.AddressList[0];
                IPEndPoint remoteEP = new IPEndPoint(ipAddress, port);

                // Create a TCP/IP socket.  
                Socket client = new Socket(AddressFamily.InterNetwork,
                    SocketType.Stream, ProtocolType.Tcp);
                if (_command == "0111" || _command == "0113" || _command == "0115" || _command == "0117" || _command == "011D")
                {
                    InsertEventLogs(Ip, "Begining Communication for " + command, "Successful", "0");
                }
                // Connect to the remote endpoint.                
                client.BeginConnect(remoteEP,
                    new AsyncCallback(ConnectCallback), client);
                connectDone.WaitOne(30000);
                if (ConnectFlag)
                {
                    if (_command == "0101")
                    {
                        if (IpStatus.List.ContainsKey(Ip))
                        {
                            IpStatus.List[Ip] = 0;
                        }
                        else
                        {
                            IpStatus.List.Add(Ip, 0);
                        }
                        string query = "UPDATE ACS_CONTROLLER SET CTLR_CONN_STATUS = 0, CTLR_INACTIVE_DATETIME = getdate() WHERE CTLR_IP = '" + Ip + "'";
                        _helper.ExecuteNonQuery(conn, CommandType.Text, query);
                    }
                    // Send test data to the remote device.  
                    Send(client, packet);
                    sendDone.WaitOne(30000);
                    if (SendFlag)
                    {
                        // Receive the response from the remote device.  
                        Receive(client, dtProfiles);
                        receiveDone.WaitOne(30000);
                        if (ReceivedFlag)
                        {
                            // Write the response to the console.  
                            //Console.WriteLine("Response received : {0}", response);
                            _logs.WriteLogs("Response from " + _ip + " for '" + command + "' received.", _ip, "Client", "AuditLog");
                            //_logs.WriteLogs("Response received :" + response, _ip);

                        }

                    }
                }
                else
                {
                    if (_command == "0101")
                    {
                        if (IpStatus.List.ContainsKey(Ip))
                        {
                            IpStatus.List[Ip] = IpStatus.List[Ip] + 1;
                        }
                        else
                        {
                            IpStatus.List.Add(Ip, 0);
                        }
                    }
                }

                if (IpStatus.List.ContainsKey(Ip))
                {
                    if (IpStatus.List[Ip] > 3)
                    {
                        string query = "UPDATE ACS_CONTROLLER SET CTLR_CONN_STATUS = 1, CTLR_INACTIVE_DATETIME = getdate() WHERE CTLR_IP = '" + Ip + "'";
                        _helper.ExecuteNonQuery(conn, CommandType.Text, query);
                    }
                }
                else
                {
                    IpStatus.List.Add(Ip, 0);
                }

                // Release the socket.
                //Thread.Sleep(30000);
                client.Shutdown(SocketShutdown.Both);
                client.Close();

            }
            catch (Exception e)
            {
                Error = e.Message;                
                _logs.ExceptionCaught(e.Message + " for " + _ip, _ip, "AsynchronousClient.StartClient", "Client", "ErrorLog");                
                returnFlag.Flag = false;
                returnFlag.Error = e.Message;
            }
            return returnFlag;
        }

        private void ConnectCallback(IAsyncResult ar)
        {
            try
            {
                // Retrieve the socket from the state object.  
                Socket client = (Socket)ar.AsyncState;

                // Complete the connection.  
                client.EndConnect(ar);

                //Console.WriteLine("Socket connected to {0}",
                //    client.RemoteEndPoint.ToString());
                _logs.WriteLogs("Socket connected to " + client.RemoteEndPoint.ToString(), _ip, "Client", "AuditLog");
                if (_command == "0111" || _command == "0113" || _command == "0115" || _command == "0117" || _command == "011D")
                {
                    InsertEventLogs(((IPEndPoint)client.RemoteEndPoint).Address.ToString(), "Connected For " + _command, "Successful", EventLog);
                }
                // Signal that the connection has been made.  
                connectDone.Set();
                ConnectFlag = true;
            }
            catch (Exception e)
            {
                Error = e.Message;
                _logs.ExceptionCaught(e.Message + " for " + _ip, _ip, "AsynchronousClient.ConnectCallback", "Client", "ErrorLog");
                _logs.WriteLogs("Socket not connected to " + _ip + " check Error Log", _ip, "Client", "AuditLog");
                if (_command == "0111" || _command == "0113" || _command == "0115" || _command == "0117" || _command == "011D")
                {
                    InsertEventLogs(_ip, "Not Connected For " + _command, "Failed", EventLog);
                }                
                
                //connectDone.Set();
                ConnectFlag = false;
                returnFlag.Flag = false;
                returnFlag.Error = e.Message;
            }
        }

        private void Receive(Socket client, DataTable Profiles = null)
        {
            try
            {
                // Create the state object.  
                StateObject state = new StateObject();
                state.workSocket = client;
                state.dt = Profiles;
                _logs.WriteLogs("Receiving " + _command + " from " + ((IPEndPoint)client.RemoteEndPoint).Address, _ip, "Client", "AuditLog");
                if (_command == "0111" || _command == "0113" || _command == "0115" || _command == "0117" || _command == "011D")
                {
                    InsertEventLogs(((IPEndPoint)client.RemoteEndPoint).Address.ToString(), "Receiving For " + _command, "Successful", EventLog);
                }
                // Begin receiving the data from the remote device.  
                client.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0,
                    new AsyncCallback(ReceiveCallback), state);
            }
            catch (Exception e)
            {
                Error = e.Message;
                _logs.ExceptionCaught(e.Message + " for " + _ip, _ip, "AsynchronousClient.Receive", "Client", "ErrorLog");
                _logs.WriteLogs("Receiving failed for " + _command + " from : " + ((IPEndPoint)client.RemoteEndPoint).Address + " check Error Log", _ip, "Client", "AuditLog");
                if (_command == "0111" || _command == "0113" || _command == "0115" || _command == "0117" || _command == "011D")
                {
                    InsertEventLogs(_ip, "Not Receiving For " + _command, "Failed", EventLog);
                }                
                returnFlag.Flag = false;
                returnFlag.Error = e.Message;

            }
        }

        private void ReceiveCallback(IAsyncResult ar)
        {
            try
            {
                // Retrieve the state object and the client socket   
                // from the asynchronous state object.  
                StateObject state = (StateObject)ar.AsyncState;
                Socket client = state.workSocket;

                // Read data from the remote device.  
                int bytesRead = client.EndReceive(ar);

                if (bytesRead > 0)
                {
                    // There might be more data, so store the data received so far.  
                    state.sb.Append(Encoding.ASCII.GetString(state.buffer, 0, bytesRead));

                    // Get the rest of the data.  
                    //client.BeginReceive(state.buffer, 0, StateObject.BufferSize, 0,
                        //new AsyncCallback(ReceiveCallback), state);
                    if (state.sb.Length > 1)
                    {
                        response = state.sb.ToString();
                    }
                    _logs.WriteLogs("Received not all " + _command + " From : " + ((IPEndPoint)client.RemoteEndPoint).Address, _ip, "Client", "AuditLog");
                    if (_command == "0111" || _command == "0113" || _command == "0115" || _command == "0117" || _command == "011D")
                    {
                        InsertEventLogs(((IPEndPoint)client.RemoteEndPoint).Address.ToString(), "Received Ack for " + _command, "Successful", EventLog, _helper);
                    }
                    if ((state.buffer[10] << 8 | state.buffer[11]) == 268)
                    {
                        // response for Download Device Configuration
                    }
                    else if ((state.buffer[10] << 8 | state.buffer[11]) == 274)
                    {
                        // response for Configure Access Level
                        if (state.dt != null)
                        {
                            DataTable UpdateAccessLevel = state.dt;
                            for (int i = 0; i < UpdateAccessLevel.Rows.Count; i++)
                            {                                
                                string query = "UPDATE ACS_ACCESSLEVEL_RELATION SET Action_flag = -1 WHERE AL_ID = '" + UpdateAccessLevel.Rows[i]["AL_ID"].ToString() + "' AND CONTROLLER_ID = '" + UpdateAccessLevel.Rows[i]["CONTROLLER_ID"].ToString() + "'";
                                _helper.ExecuteNonQuery(conn, CommandType.Text, query);
                            }
                        }
                    }
                    else if ((state.buffer[10] << 8 | state.buffer[11]) == 276)
                    {
                        // response for Configure Timezone
                        if (state.dt != null)
                        {
                            DataTable UpdateTimeZone = state.dt;
                            for (int i = 0; i < UpdateTimeZone.Rows.Count; i++)
                            {                               
                                string query = "UPDATE ACS_TIMEZONE_RELATION SET TZ_Flag = -1 WHERE TZ_CODE = '" + UpdateTimeZone.Rows[i]["TimeZoneID"].ToString() + "'";
                                _helper.ExecuteNonQuery(conn, CommandType.Text, query);
                            }
                        }
                    }
                    else if ((state.buffer[10] << 8 | state.buffer[11]) == 258)
                    {
                        // response for Get Device Status
                        //if ((state.buffer[12] << 8 | state.buffer[13]) == 00)
                        //{
                            string FirmwareRevision = Encoding.ASCII.GetString(state.buffer, 15, 16);
                            string HardwareRevision = Encoding.ASCII.GetString(state.buffer, 31, 16);
                            string DeviceMACId = Encoding.ASCII.GetString(state.buffer, 47, 17);
                            uint EventsStored = ((uint)state.buffer[64] << 8) | (uint)state.buffer[65];
                            uint MaxTransaction = (uint)state.buffer[66];
                            MaxTransaction = MaxTransaction << 24;
                            MaxTransaction = MaxTransaction | ((uint)state.buffer[67] << 16);
                            MaxTransaction = MaxTransaction | ((uint)state.buffer[68] << 8);
                            MaxTransaction |= (uint)state.buffer[69];
                            uint CurrentUserCount = ((uint)state.buffer[70] << 8) | (uint)state.buffer[71];
                            uint MaxUsers = ((uint)state.buffer[72] << 8) | (uint)state.buffer[73];                           
                            string query = "update ACS_CONTROLLER set CTLR_FIRMWARE_VERSION_NO = '" + FirmwareRevision.Trim() + "', CTLR_HARDWARE_VERSION_NO = '" + HardwareRevision + "', CTLR_MAC_ID = '" + DeviceMACId + "', CTLR_EVENTS_STORED = '" + EventsStored + "', CTLR_MAX_TRANSACTIONS = '" + MaxTransaction + "', CTLR_CURRENT_USER_CNT = '" + CurrentUserCount + "', CTLR_MAX_USER_CNT = '" + MaxUsers + "' where CTLR_IP = '" + ((IPEndPoint)client.RemoteEndPoint).Address + "'";
                            _helper.ExecuteNonQuery(conn, CommandType.Text, query);
                        //}
                    }
                    // Signal that all bytes have been received.  
                    receiveDone.Set();

                }
                else
                {
                    // All the data has arrived; put it in response.  
                    if (state.sb.Length > 1)
                    {
                        response = state.sb.ToString();
                    }
                    _logs.WriteLogs("Received nothing for " + _command + " From : " + ((IPEndPoint)client.RemoteEndPoint).Address, _ip, "Client", "AuditLog");
                    // Signal that all bytes have been received.  
                    receiveDone.Set();
                }

                ReceivedFlag = true;
                returnFlag.Flag = true;
                returnFlag.Error = string.Empty;
            }
            catch (Exception e)
            {
                Error = e.Message;
                _logs.ExceptionCaught(e.Message + " for " + _ip, _ip, "AsynchronousClient.ReceiveCallback", "Client", "ErrorLog");
                _logs.WriteLogs("Receiving call back failed for " + _command + " from : " + _ip + " check Error Log", _ip, "Client", "AuditLog");
               
                
                ReceivedFlag = false;
                returnFlag.Flag = false;
                returnFlag.Error = e.Message;
            }
        }

        private void Send(Socket client, byte[] data)
        {
            // Convert the string data to byte data using ASCII encoding.  
            byte[] byteData = data;// Encoding.ASCII.GetBytes(data);
            _logs.WriteLogs("Sending " + _command + " to : " + ((IPEndPoint)client.RemoteEndPoint).Address, _ip, "Client", "AuditLog");
            if (_command == "0111" || _command == "0113" || _command == "0115" || _command == "0117" || _command == "011D")
            {
                InsertEventLogs(((IPEndPoint)client.RemoteEndPoint).Address.ToString(), "Sending For " + _command, "Successful", EventLog);
            }
            // Begin sending the data to the remote device.  
            client.BeginSend(byteData, 0, byteData.Length, 0,
                new AsyncCallback(SendCallback), client);
        }

        private void SendCallback(IAsyncResult ar)
        {
            try
            {
                // Retrieve the socket from the state object.  
                Socket client = (Socket)ar.AsyncState;

                // Complete sending the data to the remote device.  
                int bytesSent = client.EndSend(ar);
                //Console.WriteLine("Sent {0} bytes to server.", bytesSent);
                _logs.WriteLogs("Sent " + bytesSent + " bytes to " + ((IPEndPoint)client.RemoteEndPoint).Address, _ip, "Client", "AuditLog");
                if (_command == "0111" || _command == "0113" || _command == "0115" || _command == "0117" || _command == "011D")
                {
                    InsertEventLogs(((IPEndPoint)client.RemoteEndPoint).Address.ToString(), "Sent For " + _command, "Successful", EventLog);
                }
                // Signal that all bytes have been sent.  
                sendDone.Set();
                SendFlag = true;
            }
            catch (Exception e)
            {
                Error = e.Message;
                _logs.ExceptionCaught(e.Message + " for " + _ip, _ip, "AsynchronousClient.SendCallback", "Client", "ErrorLog");
                _logs.WriteLogs("Sent failed " + _ip + " check Error Log", _ip, "Client", "AuditLog");
                SendFlag = false;
                
                returnFlag.Flag = false;
                returnFlag.Error = e.Message;
            }
        }
        public void InsertEventLogs(string ControllerID, string EventDescription, string Status, string EventID, SQLHelperClass helper = null)
        {
            try
            {
                if (helper != null)
                    _helper = helper;
                string query = "spInsertEventLogs";
                List<SqlParameter> parameters = new List<SqlParameter>();
                parameters.Add(new SqlParameter("@ControllerID", ControllerID));
                parameters.Add(new SqlParameter("@EventDescription", EventDescription));
                parameters.Add(new SqlParameter("@Status", Status));
                SqlParameter EventLogID = new SqlParameter("@EventLogID", Convert.ToInt32(EventID));
                EventLogID.Direction = ParameterDirection.InputOutput;
                parameters.Add(EventLogID);
                _helper.ExecuteProcedure(conn, query, parameters.ToArray());
                EventLog = EventLogID.Value.ToString();
            }
            catch (Exception ex)
            {
                Error = ex.Message;
                _logs.ExceptionCaught(ex.Message + " for " + _ip, _ip, "AsynchronousClient.InsertEventLogs", "Client", "AuditLog");
                
            }
        }

    }
}
