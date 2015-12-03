using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.IO;

namespace K2Field.SmartObject.Services.Azure.ServiceBus.Data
{
    public class ASBEventHub
    {
        private ServiceAssemblyBase serviceBroker = null;

        public ASBEventHub(ServiceAssemblyBase serviceBroker)
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
            List<Property> EventHubProps = GetEventHubProperties();

            ServiceObject EventHubObject = new ServiceObject();
            EventHubObject.Name = "azureeventhub";
            EventHubObject.MetaData.DisplayName = "Azure Event Hub";
            EventHubObject.MetaData.ServiceProperties.Add("objecttype", "eventhub");
            EventHubObject.MetaData.ServiceProperties.Add("objectdiscoverytype", "genericeventhub");
            EventHubObject.Active = true;

            foreach (Property prop in EventHubProps)
            {
                EventHubObject.Properties.Add(prop);
            }
            EventHubObject.Methods.Add(CreateSendEventHubMessage(EventHubProps));
            EventHubObject.Methods.Add(CreateLoadEventHub(EventHubProps));
            EventHubObject.Methods.Add(CreateListEventHubs(EventHubProps));
            EventHubObject.Methods.Add(CreateCreateEventHub(EventHubProps));
            EventHubObject.Methods.Add(CreateCreateEventHubFullDetails(EventHubProps));
            EventHubObject.Methods.Add(CreateDeleteEventHub(EventHubProps));

            serviceBroker.Service.ServiceObjects.Add(EventHubObject);
            if (!string.IsNullOrEmpty(folder))
            {
                serviceBroker.Service.ServiceFolders[folder].Add(EventHubObject);
                //folder.Add(QueueServiceObject);
            }
        }

        private List<Property> GetEventHubProperties()
        {
            List<Property> EventHubProperties = new List<Property>();

            Property pDMTTL = new Property();
            pDMTTL.Name = "createdat";
            pDMTTL.MetaData.DisplayName = "Created At";
            pDMTTL.SoType = SoType.DateTime;
            //pDMTTL.Type = Queue.DefaultMessageTimeToLive.GetType().ToString();
            EventHubProperties.Add(pDMTTL);

            Property pDDHTW = new Property();
            pDDHTW.Name = "isreadonly";
            pDDHTW.MetaData.DisplayName = "Is Readonly";
            pDDHTW.SoType = SoType.YesNo;
            //pDDHTW.Type = Queue.DuplicateDetectionHistoryTimeWindow.GetType().ToString();
            EventHubProperties.Add(pDDHTW);

            Property pEBO = new Property();
            pEBO.Name = "messageretentionindays";
            pEBO.MetaData.DisplayName = "Message Retention In Days";
            pEBO.SoType = SoType.Number;
            //pEBO.Type = Queue.EnableBatchedOperations.GetType().ToString();
            EventHubProperties.Add(pEBO);

            Property pEDL = new Property();
            pEDL.Name = "partitioncount";
            pEDL.MetaData.DisplayName = "Partition Count ";
            pEDL.SoType = SoType.Number;
            //pEDL.Type = Queue.EnableDeadLetteringOnMessageExpiration.GetType().ToString();
            EventHubProperties.Add(pEDL);

            Property pIRO = new Property();
            pIRO.Name = "partitionids";
            pIRO.MetaData.DisplayName = "PartitionIds";
            pIRO.SoType = SoType.Text;
            EventHubProperties.Add(pIRO);

            Property pLD = new Property();
            pLD.Name = "path";
            pLD.MetaData.DisplayName = "path";
            pLD.SoType = SoType.Text;
            //pLD.Type = Queue.LockDuration.GetType().ToString();
            EventHubProperties.Add(pLD);

            Property pMDC = new Property();
            pMDC.Name = "status";
            pMDC.MetaData.DisplayName = "Status";
            pMDC.SoType = SoType.Text;
            //pMDC.Type = Queue.MaxDeliveryCount.GetType().ToString();
            EventHubProperties.Add(pMDC);

            Property pMM = new Property();
            pMM.Name = "updatedat";
            pMM.MetaData.DisplayName = "Updated At";
            pMM.SoType = SoType.DateTime;
            //pMM.Type = Queue.MaxSizeInMegabytes.GetType().ToString();
            EventHubProperties.Add(pMM);

            Property pP = new Property();
            pP.Name = "eventhub";
            pP.MetaData.DisplayName = "Event Hub";
            pP.SoType = SoType.Text;
            //pP.Type = Queue.Path.GetType().ToString();
            EventHubProperties.Add(pP);

            Property pRDD = new Property();
            pRDD.Name = "usermetadata";
            pRDD.MetaData.DisplayName = "UserMetadata ";
            pRDD.SoType = SoType.Memo;
            //pRDD.Type = Queue.RequiresDuplicateDetection.GetType().ToString();
            EventHubProperties.Add(pRDD);


            Property ehData = new Property();
            ehData.Name = "data";
            ehData.MetaData.DisplayName = "Data ";
            ehData.SoType = SoType.Memo;
            //ehData.Type = Queue.RequiresDuplicateDetection.GetType().ToString();
            EventHubProperties.Add(ehData);


            EventHubProperties.AddRange(ASBStandard.GetStandardReturnProperties());

            return EventHubProperties;
        }

        private Method CreateSendEventHubMessage(List<Property> EventHubProps)
        {
            Method SendEventHubMessage = new Method();
            SendEventHubMessage.Name = "sendeventhubmessage";
            SendEventHubMessage.MetaData.DisplayName = "Send Event Hub Message";
            SendEventHubMessage.Type = MethodType.Read;
            SendEventHubMessage.InputProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());
            SendEventHubMessage.Validation.RequiredProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());
            SendEventHubMessage.InputProperties.Add(EventHubProps.Where(p => p.Name == "data").First());
            SendEventHubMessage.Validation.RequiredProperties.Add(EventHubProps.Where(p => p.Name == "data").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                SendEventHubMessage.InputProperties.Add(prop);
            }

            SendEventHubMessage.ReturnProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());
            SendEventHubMessage.ReturnProperties.Add(EventHubProps.Where(p => p.Name == "data").First());
            SendEventHubMessage.ReturnProperties.Add(EventHubProps.Where(p => p.Name == "responsestatus").First());
            SendEventHubMessage.ReturnProperties.Add(EventHubProps.Where(p => p.Name == "responsestatusdescription").First());

            return SendEventHubMessage;
        }


        private Method CreateLoadEventHub(List<Property> EventHubProps)
        {
            Method LoadEventHub = new Method();
            LoadEventHub.Name = "loadeventhub";
            LoadEventHub.MetaData.DisplayName = "Load Event Hub";
            LoadEventHub.Type = MethodType.Read;
            LoadEventHub.InputProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());
            LoadEventHub.Validation.RequiredProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                LoadEventHub.InputProperties.Add(prop);
            }

            foreach (Property prop in EventHubProps)
            {
                LoadEventHub.ReturnProperties.Add(prop);
            }
            return LoadEventHub;
        }

        private Method CreateCreateEventHubFullDetails(List<Property> EventHubProps)
        {
            Method CreateEventHub = new Method();
            CreateEventHub.Name = "createeventhubfulldetails";
            CreateEventHub.MetaData.DisplayName = "Create Event Hub Full Details";
            CreateEventHub.Type = MethodType.Read;
            CreateEventHub.InputProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());
            CreateEventHub.InputProperties.Add(EventHubProps.Where(p => p.Name.Equals("messageretentionindays", StringComparison.OrdinalIgnoreCase)).First());
            CreateEventHub.InputProperties.Add(EventHubProps.Where(p => p.Name.Equals("partitioncount", StringComparison.OrdinalIgnoreCase)).First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                CreateEventHub.InputProperties.Add(prop);
            }

            CreateEventHub.Validation.RequiredProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "defaultmessagetimetolive").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "duplicatedetectionhistorytimewindow").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "lockduration").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "maxsizeinmegabytes").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "enabledeadletteringonmessageexpiration").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "requiresduplicatedetection").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "requiressession").First());

            foreach (Property prop in EventHubProps)
            {
                CreateEventHub.ReturnProperties.Add(prop);
            }
            return CreateEventHub;
        }

        private Method CreateCreateEventHub(List<Property> EventHubProps)
        {
            Method CreateEventHub = new Method();
            CreateEventHub.Name = "createeventhub";
            CreateEventHub.MetaData.DisplayName = "Create Event Hub";
            CreateEventHub.Type = MethodType.Read;
            CreateEventHub.InputProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());
            CreateEventHub.Validation.RequiredProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                CreateEventHub.InputProperties.Add(prop);
            }

            foreach (Property prop in EventHubProps)
            {
                CreateEventHub.ReturnProperties.Add(prop);
            }
            return CreateEventHub;
        }

        private Method CreateDeleteEventHub(List<Property> EventHubProps)
        {
            Method DeleteEventHub = new Method();
            DeleteEventHub.Name = "deleteeventhub";
            DeleteEventHub.MetaData.DisplayName = "Delete Event Hub";
            DeleteEventHub.Type = MethodType.Read;
            DeleteEventHub.InputProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());
            DeleteEventHub.Validation.RequiredProperties.Add(EventHubProps.Where(p => p.Name == "eventhub").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                DeleteEventHub.InputProperties.Add(prop);
            }

            foreach (Property prop in EventHubProps)
            {
                DeleteEventHub.ReturnProperties.Add(prop);
            }
            return DeleteEventHub;
        }

        private Method CreateListEventHubs(List<Property> EventHubProps)
        {
            Method ListEventHubs = new Method();
            ListEventHubs.Name = "listeventhubs";
            ListEventHubs.MetaData.DisplayName = "List Event Hubs";
            ListEventHubs.Type = MethodType.List;

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                ListEventHubs.InputProperties.Add(prop);
            }

            foreach (Property prop in EventHubProps)
            {
                ListEventHubs.ReturnProperties.Add(prop);
            }
            return ListEventHubs;
        }

        #endregion Definition

        #region Execution


        public void SendEventHubMessage(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            
            EventHubClient Client = null;
            EventData eventData = null;
            try
            {
                string eventhubpath = string.Empty;
                if (inputs.Length == 0)
                {
                    eventhubpath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    eventhubpath = inputs.Where(p => p.Name.Equals("eventhub")).First().Value.ToString();
                }

                if (inputs.Where(p => p.Name.Equals("data")).Count() > 0)
                {
                    string msgBody = inputs.Where(p => p.Name.Equals("data")).First().Value.ToString();
                    eventData = new EventData(new MemoryStream(Encoding.UTF8.GetBytes(msgBody)));
                }
                else
                {
                    throw new Exception("Data is required to send an Event Hub Message");
                }

                Client = serviceUtilities.GetEventHubClient(eventhubpath);
                Client.Send(eventData);


                //foreach (Property prop in returns)
                //{
                //    switch (prop.Name)
                //    {
                //        case "createdat":
                //            prop.Value = Client.CreatedAt;
                //            break;
                //        case "isreadonly":
                //            prop.Value = Client.IsReadOnly;
                //            break;
                //        case "messageretentionindays":
                //            prop.Value = Client.MessageRetentionInDays;
                //            break;
                //        case "partitioncount":
                //            prop.Value = Client.PartitionCount;
                //            break;
                //        case "partitionids":
                //            prop.Value = string.Join(",", Client.PartitionIds);
                //            break;
                //        case "path":
                //            prop.Value = Client.Path;
                //            break;
                //        case "status":
                //            prop.Value = Client.Status.ToString();
                //            break;
                //        case "updatedat":
                //            prop.Value = Client.UpdatedAt;
                //            break;
                //        case "usermetadata":
                //            prop.Value = Client.UserMetadata;
                //            break;
                //    }
                //}

                returns.Where(p => p.Name.Equals("eventhub")).First().Value = inputs.Where(p => p.Name.Equals("eventhub")).First().Value.ToString();
                returns.Where(p => p.Name.Equals("data")).First().Value = inputs.Where(p => p.Name.Equals("data")).First().Value.ToString();
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Event Hub message sent";
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                try
                {
                    Client.Close();
                }
                catch { }
                Client = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void LoadEventHub(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            NamespaceManager namespaceManager = null;
            EventHubDescription EventHub = null;
            try
            {
                string eventhubpath = string.Empty;
                if (inputs.Length == 0)
                {
                    eventhubpath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    eventhubpath = inputs.Where(p => p.Name.Equals("eventhub")).First().Value.ToString();
                }
                
                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                //namespaceManager = NamespaceManager.CreateFromConnectionString(serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.ConnectionString].ToString());
                
                EventHub = namespaceManager.GetEventHub(eventhubpath);

                foreach (Property prop in returns)
                {
                    switch (prop.Name)
                    {
                        case "createdat":
                            prop.Value = EventHub.CreatedAt;
                            break;
                        case "isreadonly":
                            prop.Value = EventHub.IsReadOnly;
                            break;
                        case "messageretentionindays":
                            prop.Value = EventHub.MessageRetentionInDays;
                            break;
                        case "partitioncount":
                            prop.Value = EventHub.PartitionCount;
                            break;
                        case "partitionids":
                            prop.Value = string.Join(",", EventHub.PartitionIds);
                            break;
                        case "path":
                            prop.Value = EventHub.Path;
                            break;
                        case "status":
                            prop.Value = EventHub.Status.ToString();
                            break;
                        case "updatedat":
                            prop.Value = EventHub.UpdatedAt;
                            break;
                        case "usermetadata":
                            prop.Value = EventHub.UserMetadata;
                            break;
                    }
                }
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Event Hub loaded";
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                namespaceManager = null;
                EventHub = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ListEventHubs(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            NamespaceManager namespaceManager = null;
            System.Data.DataRow dr;
            try
            {
                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                IEnumerable<EventHubDescription> EHs = namespaceManager.GetEventHubs();

                foreach (EventHubDescription EventHub in EHs)
                {
                    dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                    foreach (Property prop in returns)
                    {
                        switch (prop.Name)
                        {
                            case "createdat":
                                dr[prop.Name] = EventHub.CreatedAt;
                                break;
                            case "isreadonly":
                                dr[prop.Name] = EventHub.IsReadOnly;
                                break;
                            case "messageretentionindays":
                                prop.Value = EventHub.MessageRetentionInDays;
                                break;
                            case "partitioncount":
                                dr[prop.Name] = EventHub.PartitionCount;
                                break;
                            case "partitionids":
                                dr[prop.Name] = string.Join(",", EventHub.PartitionIds);
                                break;
                            case "path":
                                dr[prop.Name] = EventHub.Path;
                                break;
                            case "status":
                                dr[prop.Name] = EventHub.Status.ToString();
                                break;
                            case "updatedat":
                                dr[prop.Name] = EventHub.UpdatedAt;
                                break;
                            case "usermetadata":
                                dr[prop.Name] = EventHub.UserMetadata;
                                break;
                            case "eventhub":
                                dr[prop.Name] = EventHub.Path;
                                break;
                        }
                    }
                    dr["responsestatus"] = ResponseStatus.Success;
                    dr["responsestatusdescription"] = "Event Hubs listed";
                    serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
                }
            }
            catch (Exception ex)
            {
                dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                dr["responsestatus"] = ResponseStatus.Error;
                dr["responsestatusdescription"] = ex.Message;
                serviceBroker.ServicePackage.ResultTable.Rows.Add(dr);
                return;
            }
            finally
            {
                namespaceManager = null;
            }
        }

        public void CreateEventHubs(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();

            NamespaceManager namespaceManager = null;
            EventHubDescription ReturnEventHub = null;
            try
            {
                string eventhubpath = string.Empty;
                if (inputs.Length == 0)
                {
                    eventhubpath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    eventhubpath = inputs.Where(p => p.Name.Equals("eventhub")).First().Value.ToString();
                }

                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());

                if (serviceObject.Methods[0].Name.Equals("createeventhubfulldetails"))
                {
                    EventHubDescription EventHub = new EventHubDescription(eventhubpath);

                    foreach (Property prop in inputs)
                    {
                        if (prop.Value != null)
                        {
                            switch (prop.Name)
                            {
                                case "eventhub":
                                    EventHub.Path = eventhubpath;
                                    break;
                                case "partitioncount":
                                    EventHub.PartitionCount = int.Parse(prop.Value.ToString());
                                    break;
                                case "messageretentionindays":
                                    EventHub.MessageRetentionInDays = int.Parse(prop.Value.ToString());
                                    break;
                            }
                        }
                    }

                    ReturnEventHub = namespaceManager.CreateEventHub(EventHub);
                }
                else
                {
                    // create with defaults
                    ReturnEventHub = namespaceManager.CreateEventHub(eventhubpath);
                }

                foreach (Property prop in returns)
                {
                    switch (prop.Name)
                    {
                        case "createdat":
                            prop.Value = ReturnEventHub.CreatedAt;
                            break;
                        case "isreadonly":
                            prop.Value = ReturnEventHub.IsReadOnly;
                            break;
                        case "messageretentionindays":
                            prop.Value = ReturnEventHub.MessageRetentionInDays;
                            break;
                        case "partitioncount":
                            prop.Value = ReturnEventHub.PartitionCount;
                            break;
                        case "partitionids":
                            prop.Value = string.Join(",", ReturnEventHub.PartitionIds);
                            break;
                        case "path":
                            prop.Value = ReturnEventHub.Path;
                            break;
                        case "status":
                            prop.Value = ReturnEventHub.Status.ToString();
                            break;
                        case "updatedat":
                            prop.Value = ReturnEventHub.UpdatedAt;
                            break;
                        case "usermetadata":
                            prop.Value = ReturnEventHub.UserMetadata;
                            break;
                    }
                }
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Event Hub created";

            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                namespaceManager = null;
            }

            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void DeleteEventHubs(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            NamespaceManager namespaceManager = null;
            try
            {
                string eventhubpath = string.Empty;
                if (inputs.Length == 0)
                {
                    eventhubpath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    eventhubpath = inputs.Where(p => p.Name.Equals("eventhub")).First().Value.ToString();
                }

                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                namespaceManager.DeleteEventHub(eventhubpath);
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Event Hub deleted";

            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                namespaceManager = null;
            }

            serviceObject.Properties.BindPropertiesToResultTable();
        }


        #endregion Execution
    }
}
