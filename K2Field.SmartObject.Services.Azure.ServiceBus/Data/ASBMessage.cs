using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;
using SourceCode.SmartObjects.Services.ServiceSDK;

using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Transactions;
using System.IO;

namespace K2Field.SmartObject.Services.Azure.ServiceBus.Data
{
    public class ASBMessage
    {
        private ServiceAssemblyBase serviceBroker = null;       

        public ASBMessage(ServiceAssemblyBase serviceBroker)
        {
            // Set local serviceBroker variable.
            this.serviceBroker = serviceBroker;
        }

        #region Definition
        
        public void Create()
        {
            Create(string.Empty);
        }

        public void Create(string folder)
        {
            List<Property> MessageProps = GetMessagePropertiesReturn();

            ServiceObject MessageServiceObject = new ServiceObject();
            MessageServiceObject.Name = "azuremessage";
            MessageServiceObject.MetaData.DisplayName = "Azure Message";
            MessageServiceObject.MetaData.ServiceProperties.Add("objecttype", "message");
            MessageServiceObject.MetaData.ServiceProperties.Add("objectdiscoverytype", "genericmessage");
            MessageServiceObject.Active = true;
            foreach (Property prop in MessageProps)
            {
                MessageServiceObject.Properties.Add(prop);
            }
            MessageServiceObject.Methods.Add(CreateSendMessage(MessageProps));
            MessageServiceObject.Methods.Add(CreateReceiveMessage(MessageProps));
            MessageServiceObject.Methods.Add(CreatePeekLockMessage(MessageProps));
            MessageServiceObject.Methods.Add(CreateReceivePeekLockMessage(MessageProps));
            MessageServiceObject.Methods.Add(CreateSessionReceiveMessage(MessageProps));
            MessageServiceObject.Methods.Add(CreateAbandonMessage(MessageProps));
            //MessageServiceObject.Methods.Add(CreateDeadLetterMessage(MessageProps));

            serviceBroker.Service.ServiceObjects.Add(MessageServiceObject);

            if (!string.IsNullOrEmpty(folder))
            {
                serviceBroker.Service.ServiceFolders[folder].Add(MessageServiceObject);
            }
        }

        public List<Property> GetMessagePropertiesReturn()
        {
            List<Property> MessageProperties = new List<Property>();
            //            BrokeredMessage Message = new BrokeredMessage("test");

            Property p0 = new Property();
            p0.Name = "path";
            p0.MetaData.DisplayName = "Path";
            p0.SoType = SoType.Text;
            p0.Type = "string";
            MessageProperties.Add(p0);

            //Property p01 = new Property();
            //p01.Name = "requesttimeout";
            //p01.MetaData.DisplayName = "Request Timeout";
            //p01.Value = 60;
            //p01.SoType = SoType.Number;
            //MessageProperties.Add(p01);

            Property p1 = new Property();
            p1.Name = "contenttype";
            p1.MetaData.DisplayName = "Content Type";
            p1.SoType = SoType.Text;
            //p1.Type = Message.ContentType.GetType().ToString();
            MessageProperties.Add(p1);

            Property p2 = new Property();
            p2.Name = "correlationid";
            p2.MetaData.DisplayName = "Correlation Id";
            p2.SoType = SoType.Text;
            //p2.Type = Message.CorrelationId.GetType().ToString();
            MessageProperties.Add(p2);

            Property p3 = new Property();
            p3.Name = "deliverycount";
            p3.MetaData.DisplayName = "Delivery Count";
            p3.SoType = SoType.Number;
            //p3.Type = Message.DeliveryCount.GetType().ToString();
            MessageProperties.Add(p3);

            Property p4 = new Property();
            p4.Name = "enqueuedtimeutc";
            p4.MetaData.DisplayName = "Enqueued Time Utc";
            p4.SoType = SoType.DateTime;
            //p4.Type = Message.EnqueuedTimeUtc.GetType().ToString();
            MessageProperties.Add(p4);

            Property p5 = new Property();
            p5.Name = "expiresatutc";
            p5.MetaData.DisplayName = "Expires At Utc";
            p5.SoType = SoType.Decimal;
            //p5.Type = Message.ExpiresAtUtc.GetType().ToString();
            MessageProperties.Add(p5);

            Property p6 = new Property();
            p6.Name = "label";
            p6.MetaData.DisplayName = "Label";
            p6.SoType = SoType.Text;
            //p6.Type = Message.Label.GetType().ToString();
            MessageProperties.Add(p6);

            Property p7 = new Property();
            p7.Name = "messageid";
            p7.MetaData.DisplayName = "Message Id";
            p7.SoType = SoType.Text;
            //p7.Type = Message.MessageId.GetType().ToString();
            MessageProperties.Add(p7);

            Property p8 = new Property();
            p8.Name = "properties";
            p8.MetaData.DisplayName = "Properties";
            p8.SoType = SoType.Text;
            //p8.Type = "IDictionary<string, object>";
            MessageProperties.Add(p8);

            Property p9 = new Property();
            p9.Name = "replyto";
            p9.MetaData.DisplayName = "Reply To";
            p9.SoType = SoType.Text;
            //p9.Type = Message.ReplyTo.GetType().ToString();
            MessageProperties.Add(p9);

            Property p10 = new Property();
            p10.Name = "replytosessionid";
            p10.MetaData.DisplayName = "Reply To Session Id";
            p10.SoType = SoType.Text;
            //p10.Type = Message.ReplyToSessionId.GetType().ToString();
            MessageProperties.Add(p10);

            Property p11 = new Property();
            p11.Name = "scheduledenqueuetimeutc";
            p11.MetaData.DisplayName = "Scheduled Enqueue Time Utc";
            p11.SoType = SoType.Decimal;
            //p11.Type = Message.ScheduledEnqueueTimeUtc.GetType().ToString();
            MessageProperties.Add(p11);

            Property p12 = new Property();
            p12.Name = "sequencenumber";
            p12.MetaData.DisplayName = "Sequence Number";
            p12.SoType = SoType.Number;
            //p12.Type = Message.SequenceNumber.GetType().ToString();
            MessageProperties.Add(p12);

            Property p13 = new Property();
            p13.Name = "sessionid";
            p13.MetaData.DisplayName = "Session Id";
            p13.SoType = SoType.Text;
            //p13.Type = Message.SessionId.GetType().ToString();
            MessageProperties.Add(p13);

            Property p14 = new Property();
            p14.Name = "size";
            p14.MetaData.DisplayName = "Size";
            p14.SoType = SoType.Number;
            //p14.Type = Message.Size.GetType().ToString();
            MessageProperties.Add(p14);

            Property p15 = new Property();
            p15.Name = "timetolive";
            p15.MetaData.DisplayName = "Time To Live";
            p15.SoType = SoType.Decimal;
            //p15.Type = Message.TimeToLive.GetType().ToString();
            MessageProperties.Add(p15);

            Property p16 = new Property();
            p16.Name = "to";
            p16.MetaData.DisplayName = "To";
            p16.SoType = SoType.Text;
            //p16.Type = Message.To.GetType().ToString();
            MessageProperties.Add(p16);

            Property p17 = new Property();
            p17.Name = "body";
            p17.MetaData.DisplayName = "Body";
            p17.SoType = SoType.Text;
            //p17.Type = "string";
            MessageProperties.Add(p17);

            Property p18 = new Property();
            p18.Name = "locktoken";
            p18.MetaData.DisplayName = "Lock Token";
            p18.SoType = SoType.Guid;
            //p18.Type = "string";
            MessageProperties.Add(p18);

            MessageProperties.AddRange(ASBStandard.GetStandardReturnProperties());

            return MessageProperties;
        }

        public  List<Property> GetMessagePropertiesInput()
        {
            List<Property> MessageProperties = new List<Property>();
            //            BrokeredMessage Message = new BrokeredMessage("test");

            Property p0 = new Property();
            p0.Name = "path";
            p0.MetaData.DisplayName = "Path";
            p0.SoType = SoType.Text;
            p0.Type = "string";
            MessageProperties.Add(p0);

            MessageProperties.AddRange(ASBStandard.GetStandardInputProperties());

            Property p1 = new Property();
            p1.Name = "contenttype";
            p1.MetaData.DisplayName = "Content Type";
            p1.SoType = SoType.Text;
            //p1.Type = Message.ContentType.GetType().ToString();
            MessageProperties.Add(p1);

            Property p2 = new Property();
            p2.Name = "correlationid";
            p2.MetaData.DisplayName = "Correlation Id";
            p2.SoType = SoType.Text;
            //p2.Type = Message.CorrelationId.GetType().ToString();
            MessageProperties.Add(p2);

            Property p6 = new Property();
            p6.Name = "label";
            p6.MetaData.DisplayName = "Label";
            p6.SoType = SoType.Text;
            //p6.Type = Message.Label.GetType().ToString();
            MessageProperties.Add(p6);

            Property p7 = new Property();
            p7.Name = "messageid";
            p7.MetaData.DisplayName = "Message Id";
            p7.SoType = SoType.Text;
            //p7.Type = Message.MessageId.GetType().ToString();
            MessageProperties.Add(p7);

            Property p8 = new Property();
            p8.Name = "properties";
            p8.MetaData.DisplayName = "Properties";
            p8.SoType = SoType.Text;
            //p8.Type = "IDictionary<string, object>";
            MessageProperties.Add(p8);

            Property p9 = new Property();
            p9.Name = "replyto";
            p9.MetaData.DisplayName = "Reply To";
            p9.SoType = SoType.Text;
            //p9.Type = Message.ReplyTo.GetType().ToString();
            MessageProperties.Add(p9);

            Property p10 = new Property();
            p10.Name = "replytosessionid";
            p10.MetaData.DisplayName = "Reply To Session Id";
            p10.SoType = SoType.Text;
            //p10.Type = Message.ReplyToSessionId.GetType().ToString();
            MessageProperties.Add(p10);

            Property p13 = new Property();
            p13.Name = "sessionid";
            p13.MetaData.DisplayName = "Session Id";
            p13.SoType = SoType.Text;
            //p13.Type = Message.SessionId.GetType().ToString();
            MessageProperties.Add(p13);

            Property p15 = new Property();
            p15.Name = "timetolive";
            p15.MetaData.DisplayName = "Time To Live";
            p15.SoType = SoType.Number;
            //p15.Type = Message.TimeToLive.GetType().ToString();
            MessageProperties.Add(p15);

            Property p16 = new Property();
            p16.Name = "to";
            p16.MetaData.DisplayName = "To";
            p16.SoType = SoType.Text;
            //p16.Type = Message.To.GetType().ToString();
            MessageProperties.Add(p16);

            Property p17 = new Property();
            p17.Name = "body";
            p17.MetaData.DisplayName = "Body";
            p17.SoType = SoType.Text;
            //p17.Type = "string";
            MessageProperties.Add(p17);

            return MessageProperties;
        }

        public Method CreateSendMessage(List<Property> MessageProps)
        {
            Method SendMessage = new Method();
            SendMessage.Name = "sendmessage";
            SendMessage.MetaData.DisplayName = "Send Message";
            SendMessage.Type = MethodType.Read;
            foreach (Property prop in GetMessagePropertiesInput())
            {
                SendMessage.InputProperties.Add(prop);
            }
            SendMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());

            foreach (Property prop in MessageProps)
            {
                SendMessage.ReturnProperties.Add(prop);
            }


            return SendMessage;
        }

        public Method CreateReceiveMessage(List<Property> MessageProps)
        {
            Method ReceiveMessage = new Method();
            ReceiveMessage.Name = "receivemessage";
            ReceiveMessage.MetaData.DisplayName = "Receive Message";
            ReceiveMessage.Type = MethodType.Read;

            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                ReceiveMessage.InputProperties.Add(prop);
            }

            foreach (Property prop in MessageProps)
            {
                ReceiveMessage.ReturnProperties.Add(prop);
            }

            return ReceiveMessage;
        }

        public Method CreatePeekLockMessage(List<Property> MessageProps)
        {
            Method ReceiveMessage = new Method();
            ReceiveMessage.Name = "peeklockmessage";
            ReceiveMessage.MetaData.DisplayName = "Peek Lock Message";
            ReceiveMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                ReceiveMessage.ReturnProperties.Add(prop);
            }
            // remove body from peek lock
            ReceiveMessage.ReturnProperties.Remove(MessageProps.Where(p => p.Name == "body").First());

            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                ReceiveMessage.InputProperties.Add(prop);
            }

            return ReceiveMessage;
        }

        public Method CreateReceivePeekLockMessage(List<Property> MessageProps)
        {
            Method ReceiveMessage = new Method();
            ReceiveMessage.Name = "receivepeeklockmessage";
            ReceiveMessage.MetaData.DisplayName = "Recieve Peek Lock Message";
            ReceiveMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                ReceiveMessage.ReturnProperties.Add(prop);
            }

            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());

            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                ReceiveMessage.InputProperties.Add(prop);
            }

            return ReceiveMessage;
        }

        public Method CreateSessionReceiveMessage(List<Property> MessageProps)
        {
            Method ReceiveMessage = new Method();
            ReceiveMessage.Name = "receivesessionmessage";
            ReceiveMessage.MetaData.DisplayName = "Receive Session Message";
            ReceiveMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                ReceiveMessage.ReturnProperties.Add(prop);
            }

            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "replytosessionid").First());

            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            ReceiveMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "replytosessionid").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                ReceiveMessage.InputProperties.Add(prop);
            }

            return ReceiveMessage;
        }

        public Method CreateAbandonMessage(List<Property> MessageProps)
        {
            Method AbandonMessage = new Method();
            AbandonMessage.Name = "abandonmessage";
            AbandonMessage.MetaData.DisplayName = "Abandon Message";
            AbandonMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                AbandonMessage.ReturnProperties.Add(prop);
            }
            // remove body from peek lock
            AbandonMessage.ReturnProperties.Remove(MessageProps.Where(p => p.Name == "body").First());

            AbandonMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            //AbandonMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());
            AbandonMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "locktoken").First());


            AbandonMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            //AbandonMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());
            AbandonMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "locktoken").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                AbandonMessage.InputProperties.Add(prop);
            }

            return AbandonMessage;
        }

        public Method CreateDeadLetterMessage(List<Property> MessageProps)
        {
            Method DeadLetterMessage = new Method();
            DeadLetterMessage.Name = "deadletternmessage";
            DeadLetterMessage.MetaData.DisplayName = "Dead Letter Message";
            DeadLetterMessage.Type = MethodType.Read;
            foreach (Property prop in MessageProps)
            {
                DeadLetterMessage.ReturnProperties.Add(prop);
            }
            // remove body from peek lock
            DeadLetterMessage.ReturnProperties.Remove(MessageProps.Where(p => p.Name == "body").First());

            DeadLetterMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            //DeadLetterMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());
            DeadLetterMessage.InputProperties.Add(MessageProps.Where(p => p.Name == "locktoken").First());


            DeadLetterMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "path").First());
            //DeadLetterMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "sequencenumber").First());
            DeadLetterMessage.Validation.RequiredProperties.Add(MessageProps.Where(p => p.Name == "locktoken").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                DeadLetterMessage.InputProperties.Add(prop);
            }

            return DeadLetterMessage;
        }

        #endregion Definition

        #region Execution

        public void SendMessage(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            serviceObject.Properties.InitResultTable();
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            try
            {
                MessagingFactory factory = factory = serviceUtilities.GetMessagingFactory(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                MessageSender MsgSender;
                if (inputs.Where(p => p.Name.Equals("path")).Count() > 0)
                {
                    MsgSender = factory.CreateMessageSender(inputs.Where(p => p.Name.Equals("path")).First().Value.ToString());
                }
                else
                {
                    throw new Exception("Send Message Failed: No queue defined");
                }

                BrokeredMessage msg;
                if (inputs.Where(p => p.Name.Equals("body")).Count() > 0)
                {
                    string msgBody = inputs.Where(p => p.Name.Equals("body")).First().Value.ToString();
                    msg = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(msgBody)));
                }
                else
                {
                    msg = new BrokeredMessage();
                }

                foreach (Property prop in inputs)
                {
                    if (prop.Value != null)
                    {
                        switch (prop.Name)
                        {
                            case "contenttype":
                                msg.ContentType = prop.Value.ToString();
                                break;
                            case "correlationid":
                                msg.CorrelationId = prop.Value.ToString();
                                break;
                            case "label":
                                msg.Label = prop.Value.ToString();
                                break;
                            case "messageid":
                                msg.MessageId = prop.Value.ToString();
                                break;
                            case "replyto":
                                msg.ReplyTo = prop.Value.ToString();
                                break;
                            case "replytosessionid":
                                msg.ReplyToSessionId = prop.Value.ToString();
                                break;
                            case "scheduledenqueuetimeutc":
                                DateTime setu;
                                if (DateTime.TryParse(prop.Value.ToString(), out setu))
                                {
                                    msg.ScheduledEnqueueTimeUtc = setu;
                                }
                                break;
                            case "sessionid":
                                msg.SessionId = prop.Value.ToString();
                                break;
                            case "timetolive":
                                msg.TimeToLive = new TimeSpan(long.Parse(prop.Value.ToString()) * TimeSpan.TicksPerSecond);
                                break;
                            case "to":
                                msg.To = prop.Value.ToString();
                                break;
                            case "properties":
                                string[] d1 = { serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.Delimiter1].ToString() };
                                string[] properties = prop.Value.ToString().Split(d1, StringSplitOptions.RemoveEmptyEntries);
                                foreach (string a in properties)
                                {
                                    string[] d2 = { serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.Delimiter2].ToString() };
                                    string[] keyvalue = a.Split(d2, StringSplitOptions.RemoveEmptyEntries);
                                    if (keyvalue.Length == 2)
                                    {
                                        msg.Properties.Add(keyvalue[0], keyvalue[1]);
                                    }
                                }
                                break;
                        }
                    }

                }

                MsgSender.Send(msg);
                MsgSender.Close();

                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Sent;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message sent";

            }
            catch (TimeoutException tex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Timeout;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = tex.Message;

            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ReceiveMessage(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);

            serviceObject.Properties.InitResultTable();
            MessagingFactory factory = null;
            MessageReceiver msgReceiver = null;
            BrokeredMessage receivedMessage = null;
            try
            {             
                factory = serviceUtilities.GetMessagingFactory(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());

                if (inputs.Where(p => p.Name.Equals("path")).Count() > 0)
                {
                    msgReceiver = factory.CreateMessageReceiver(inputs.Where(p => p.Name.Equals("path")).First().Value.ToString(), ReceiveMode.PeekLock);
                }
                else
                {
                    throw new Exception("Receive Message Failed: No queue defined");
                }

                //Guid locktoken = Guid.Empty;
                //if (inputs.Where(p => p.Name.Equals("locktoken")).Count() > 0)
                //{
                //    locktoken = new Guid(inputs.Where(p => p.Name.Equals("locktoken")).First().Value.ToString());
                //}

                long sequencenumber = -1;
                if (inputs.Where(p => p.Name.Equals("sequencenumber")).Count() > 0)
                {
                    sequencenumber = long.Parse(inputs.Where(p => p.Name.Equals("sequencenumber")).First().Value.ToString());
                }

                if (serviceObject.Methods[0].Name.Equals("receivepeeklockmessage"))
                {
                    if (sequencenumber != -1)
                    {
                        // get next message
                        receivedMessage = msgReceiver.Receive(sequencenumber);                        
                    }
                    else
                    {
                        throw new Exception("Receive Peek Lock Message Failed: No sequence number defined");
                    }
                }
                else
                {
                    // get next message
                    receivedMessage = msgReceiver.Receive();
                }


                if (receivedMessage != null)
                {
                    // map message to return properties
                    foreach (Property prop in returns)
                    {
                        prop.Value = SetBrokeredMessagePropertyValue(prop, receivedMessage);
                    }

                    if (serviceObject.Methods[0].Name.Equals("peeklockmessage"))
                    {
                        // mark as defer
                        // use lock token to complete, abandon or deadletter
                        receivedMessage.Defer();
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Deferred;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Received and Deferred";
                    }
                    else
                    {
                        // mark as complete
                        // will completely remove it from the queue
                        receivedMessage.Complete();
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Received;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Received";
                    }
                }
                else
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.MessageNotFound;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "No message received";
                }

            }
            catch (TimeoutException tex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Timeout;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Received timed out";
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            catch (MessageNotFoundException mnfe)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.MessageNotFound;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = mnfe.Message;
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            catch (Exception ex)
            {
                //throw;
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            finally
            {
                if (factory != null)
                {
                    //factory.Close();
                    factory = null;
                }
                if (msgReceiver != null)
                {
                    //msgReceiver.Close();
                    msgReceiver = null;
                }
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ReceiveSessionMessage(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);

            serviceObject.Properties.InitResultTable();

            MessagingFactory factory = null;
            MessageReceiver msgReceiver = null;
            BrokeredMessage receivedMessage = null;
            try
            {
                factory = serviceUtilities.GetMessagingFactory(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                string path = string.Empty;
                if (inputs.Where(p => p.Name.Equals("path")).Count() > 0)
                {
                    path = inputs.Where(p => p.Name.Equals("path")).First().Value.ToString();
                }
                else
                {
                    throw new Exception("Receive Message Failed: No queue defined");
                }

                //Guid locktoken = Guid.Empty;
                //if (inputs.Where(p => p.Name.Equals("locktoken")).Count() > 0)
                //{
                //    locktoken = new Guid(inputs.Where(p => p.Name.Equals("locktoken")).First().Value.ToString());
                //}

                string correlationIdentifier = string.Empty;
                if (inputs.Where(p => p.Name.Equals("replytosessionid")).Count() > 0)
                {
                    correlationIdentifier = inputs.Where(p => p.Name.Equals("replytosessionid")).First().Value.ToString();
                }

                MessageSession msgSession = null;
                if (path.Contains("/"))
                {
                    // assume subscription
                    path = path.Replace("/Subscription", "");
                    string[] ts = path.Split('/');
                    SubscriptionClient qClient = factory.CreateSubscriptionClient(ts[0], ts[1]);
                    msgSession = qClient.AcceptMessageSession(correlationIdentifier);
                }
                else
                {
                    QueueClient qClient = factory.CreateQueueClient(path);
                    msgSession = qClient.AcceptMessageSession(correlationIdentifier);
                }

                // get one message
                receivedMessage = msgSession.Receive();

                if (receivedMessage != null)
                {
                    // map message to return properties
                    foreach (Property prop in returns)
                    {
                        prop.Value = SetBrokeredMessagePropertyValue(prop, receivedMessage);
                    }

                    if (serviceObject.Methods[0].Name.Equals("peeklockmessage"))
                    {
                        // mark as defer
                        // use lock token to complete, abandon or deadletter
                        receivedMessage.Defer();
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Deferred;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Received and Deferred";
                    }
                    else
                    {
                        // mark as complete
                        // will completely remove it from the queue
                        receivedMessage.Complete();
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Received;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Session Message Received";
                    }
                }
                else
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "No session message received";
                }

            }
            catch (TimeoutException tex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Timeout;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = tex.Message;
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            catch (MessageNotFoundException mnfe)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.MessageNotFound;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = mnfe.Message;
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            catch (Exception ex)
            {
                //throw;
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            finally
            {
                if (factory != null)
                {
                    //factory.Close();
                    factory = null;
                }
                if (msgReceiver != null)
                {
                    //msgReceiver.Close();
                    msgReceiver = null;
                }
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void AbandonMessage(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);

            serviceObject.Properties.InitResultTable();
            MessagingFactory factory = null;
            MessageReceiver msgReceiver = null;
            try
            {
                factory = serviceUtilities.GetMessagingFactory(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());

                if (inputs.Where(p => p.Name.Equals("path")).Count() > 0)
                {
                    msgReceiver = factory.CreateMessageReceiver(inputs.Where(p => p.Name.Equals("path")).First().Value.ToString(), ReceiveMode.PeekLock);
                }
                else
                {
                    throw new Exception("Receive Message Failed: No queue defined");
                }

                Guid locktoken = Guid.Empty;
                if (inputs.Where(p => p.Name.Equals("locktoken")).Count() > 0)
                {
                    locktoken = new Guid(inputs.Where(p => p.Name.Equals("locktoken")).First().Value.ToString());
                }

                //long sequencenumber = -1;
                //if (inputs.Where(p => p.Name.Equals("sequencenumber")).Count() > 0)
                //{
                //    sequencenumber = long.Parse(inputs.Where(p => p.Name.Equals("sequencenumber")).First().Value.ToString());
                //}

                if (locktoken != Guid.Empty)
                {
                    // get next message
                    msgReceiver.Abandon(locktoken);
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Abandoned";
                }
                else
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Lock Token not specified";
                }


            }
            catch (TimeoutException tex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Timeout;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Abandoned timed out";
                msgReceiver = null;
            }
            catch (MessageNotFoundException mnfe)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.MessageNotFound;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = mnfe.Message;
                msgReceiver = null;
            }
            catch (Exception ex)
            {
                //throw;
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
                msgReceiver = null;
            }
            finally
            {
                if (factory != null)
                {
                    //factory.Close();
                    factory = null;
                }
                if (msgReceiver != null)
                {
                    //msgReceiver.Close();
                    msgReceiver = null;
                }
            }
            serviceObject.Properties.BindPropertiesToResultTable();

        }

        public void DeadLetterMessage(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);

            serviceObject.Properties.InitResultTable();
            MessagingFactory factory = null;
            MessageReceiver msgReceiver = null;
            BrokeredMessage receivedMessage = null;
            try
            {
                factory = serviceUtilities.GetMessagingFactory(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());

                if (inputs.Where(p => p.Name.Equals("path")).Count() > 0)
                {
                    msgReceiver = factory.CreateMessageReceiver(inputs.Where(p => p.Name.Equals("path")).First().Value.ToString(), ReceiveMode.PeekLock);
                }
                else
                {
                    throw new Exception("Receive Message Failed: No queue defined");
                }

                //Guid locktoken = Guid.Empty;
                //if (inputs.Where(p => p.Name.Equals("locktoken")).Count() > 0)
                //{
                //    locktoken = new Guid(inputs.Where(p => p.Name.Equals("locktoken")).First().Value.ToString());
                //}

                long sequencenumber = -1;
                if (inputs.Where(p => p.Name.Equals("sequencenumber")).Count() > 0)
                {
                    sequencenumber = long.Parse(inputs.Where(p => p.Name.Equals("sequencenumber")).First().Value.ToString());
                }

                if (serviceObject.Methods[0].Name.Equals("receivepeeklockmessage"))
                {
                    if (sequencenumber != -1)
                    {
                        // get next message
                        receivedMessage = msgReceiver.Receive(sequencenumber);
                    }
                    else
                    {
                        throw new Exception("Receive Peek Lock Message Failed: No sequence number defined");
                    }
                }
                else
                {
                    // get next message
                    receivedMessage = msgReceiver.Receive();
                }


                if (receivedMessage != null)
                {
                    // map message to return properties
                    foreach (Property prop in returns)
                    {
                        prop.Value = SetBrokeredMessagePropertyValue(prop, receivedMessage);
                    }

                    if (serviceObject.Methods[0].Name.Equals("peeklockmessage"))
                    {
                        // mark as defer
                        // use lock token to complete, abandon or deadletter
                        receivedMessage.Defer();
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Deferred;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Received and Deferred";
                    }
                    else
                    {
                        // mark as complete
                        // will completely remove it from the queue
                        receivedMessage.Complete();
                        returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Received;
                        returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Received";
                    }
                }
                else
                {
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.MessageNotFound;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "No message received";
                }

            }
            catch (TimeoutException tex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Timeout;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Message Received timed out";
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            catch (MessageNotFoundException mnfe)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.MessageNotFound;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = mnfe.Message;
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            catch (Exception ex)
            {
                //throw;
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
                if (msgReceiver != null && receivedMessage != null)
                {
                    // abandon message
                    // message will be returned to the queue
                    receivedMessage.Abandon();
                }
            }
            finally
            {
                if (factory != null)
                {
                    //factory.Close();
                    factory = null;
                }
                if (msgReceiver != null)
                {
                    //msgReceiver.Close();
                    msgReceiver = null;
                }
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        private object SetBrokeredMessagePropertyValue(Property prop, BrokeredMessage msg)
        {
            try
            {
                switch (prop.Name)
                {
                    case "contenttype":
                        prop.Value = msg.ContentType;
                        break;
                    case "correlationid":
                        prop.Value = msg.CorrelationId;
                        break;
                    case "deliverycount":
                        prop.Value = msg.DeliveryCount;
                        break;
                    case "enqueuedtimeutc":
                        prop.Value = msg.EnqueuedTimeUtc;
                        break;
                    case "expiresatutc":
                        prop.Value = msg.ExpiresAtUtc;
                        break;
                    case "label":
                        prop.Value = msg.Label;
                        break;
                    case "messageid":
                        prop.Value = msg.MessageId;
                        break;
                    case "replyto":
                        prop.Value = msg.ReplyTo;
                        break;
                    case "replytosessionid":
                        prop.Value = msg.ReplyToSessionId;
                        break;
                    case "scheduledenqueuetimeutc":
                        prop.Value = msg.ScheduledEnqueueTimeUtc;
                        break;
                    case "sequencenumber":
                        prop.Value = msg.SequenceNumber;
                        break;
                    case "sessionid":
                        prop.Value = msg.SessionId;
                        break;
                    case "size":
                        prop.Value = msg.Size;
                        break;
                    case "timetolive":
                        prop.Value = (msg.TimeToLive.Ticks * TimeSpan.TicksPerSecond);
                        break;
                    case "to":
                        prop.Value = msg.To;
                        break;
                    case "body":
                        // need to sort out deserialization
                        // perhaps read from content type, create type and try and deserialize
                        //prop.Value = msg.GetBody<System.IO.Stream>();

                        try
                        {
                            prop.Value = msg.GetBody<string>();
                            break;
                        }
                        catch (Exception ex) { }

                        break;
                    case "properties":
                        // todo                      
                        string properties = string.Empty;
                        foreach (KeyValuePair<string, object> kv in msg.Properties)
                        {
                            properties += kv.Key + serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.Delimiter1].ToString() + kv.Value + serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.Delimiter2].ToString();
                        }
                        if (properties.EndsWith(serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.Delimiter2].ToString()))
                        {
                            properties.Remove(properties.Length - 1);
                        }
                        prop.Value = properties;
                        break;
                    case "locktoken":
                        prop.Value = msg.LockToken;
                        break;
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return prop.Value;
        }

        #endregion Execution
    }
}
