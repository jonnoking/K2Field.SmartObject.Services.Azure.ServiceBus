using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK;

using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;


namespace K2Field.SmartObject.Services.Azure.ServiceBus.Data
{
    public class ASBQueue
    {
        private ServiceAssemblyBase serviceBroker = null;

        public ASBQueue(ServiceAssemblyBase serviceBroker)
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
            List<Property> QueueProps = GetQueueProperties();

            ServiceObject QueueServiceObject = new ServiceObject();
            QueueServiceObject.Name = "azurequeue";
            QueueServiceObject.MetaData.DisplayName = "Azure Queue";
            QueueServiceObject.MetaData.ServiceProperties.Add("objecttype", "queue");
            QueueServiceObject.MetaData.ServiceProperties.Add("objectdiscoverytype", "genericqueue");
            QueueServiceObject.Active = true;

            foreach (Property prop in QueueProps)
            {
                QueueServiceObject.Properties.Add(prop);
            }
            QueueServiceObject.Methods.Add(CreateLoadQueue(QueueProps));
            QueueServiceObject.Methods.Add(CreateListQueues(QueueProps));
            QueueServiceObject.Methods.Add(CreateCreateQueue(QueueProps));
            QueueServiceObject.Methods.Add(CreateCreateQueueFullDetails(QueueProps));
            QueueServiceObject.Methods.Add(CreateDeleteQueue(QueueProps));

            serviceBroker.Service.ServiceObjects.Add(QueueServiceObject);
            if (!string.IsNullOrEmpty(folder))
            {
                serviceBroker.Service.ServiceFolders[folder].Add(QueueServiceObject);
                //folder.Add(QueueServiceObject);
            }
        }

        private List<Property> GetQueueProperties()
        {
            List<Property> QueueProperties = new List<Property>();           

            Property pDMTTL = new Property();
            pDMTTL.Name = "defaultmessagetimetolive";
            pDMTTL.MetaData.DisplayName = "Default Message Time To Live";
            pDMTTL.SoType = SoType.Decimal;
            //pDMTTL.Type = Queue.DefaultMessageTimeToLive.GetType().ToString();
            QueueProperties.Add(pDMTTL);

            Property pDDHTW = new Property();
            pDDHTW.Name = "duplicatedetectionhistorytimewindow";
            pDDHTW.MetaData.DisplayName = "Duplicate Detection History Time Window";
            pDDHTW.SoType = SoType.Decimal;
            //pDDHTW.Type = Queue.DuplicateDetectionHistoryTimeWindow.GetType().ToString();
            QueueProperties.Add(pDDHTW);

            Property pEBO = new Property();
            pEBO.Name = "enablebatchedoperations";
            pEBO.MetaData.DisplayName = "Enable Batched Operations";
            pEBO.SoType = SoType.YesNo;
            //pEBO.Type = Queue.EnableBatchedOperations.GetType().ToString();
            QueueProperties.Add(pEBO);

            Property pEDL = new Property();
            pEDL.Name = "enabledeadletteringonmessageexpiration";
            pEDL.MetaData.DisplayName = "Enable Dead Lettering On Message Expiration";
            pEDL.SoType = SoType.YesNo;
            //pEDL.Type = Queue.EnableDeadLetteringOnMessageExpiration.GetType().ToString();
            QueueProperties.Add(pEDL);

            Property pIRO = new Property();
            pIRO.Name = "isreadonly";
            pIRO.MetaData.DisplayName = "Is Read Only";
            pIRO.SoType = SoType.YesNo;
            QueueProperties.Add(pIRO);

            Property pLD = new Property();
            pLD.Name = "lockduration";
            pLD.MetaData.DisplayName = "Lock Duration";
            pLD.SoType = SoType.Decimal;
            //pLD.Type = Queue.LockDuration.GetType().ToString();
            QueueProperties.Add(pLD);

            Property pMDC = new Property();
            pMDC.Name = "maxdeliverycount";
            pMDC.MetaData.DisplayName = "Max Delivery Count";
            pMDC.SoType = SoType.Number;
            //pMDC.Type = Queue.MaxDeliveryCount.GetType().ToString();
            QueueProperties.Add(pMDC);

            Property pMM = new Property();
            pMM.Name = "maxsizeinmegabytes";
            pMM.MetaData.DisplayName = "Max Size In Megabytes";
            pMM.SoType = SoType.Number;
            //pMM.Type = Queue.MaxSizeInMegabytes.GetType().ToString();
            QueueProperties.Add(pMM);

            Property pMC = new Property();
            pMC.Name = "messagecount";
            pMC.MetaData.DisplayName = "Message Count";
            pMC.SoType = SoType.Number;
            //pMC.Type = Queue.MessageCount.GetType().ToString();
            QueueProperties.Add(pMC);

            Property pP = new Property();
            pP.Name = "queue";
            pP.MetaData.DisplayName = "Queue";
            pP.SoType = SoType.Text;
            //pP.Type = Queue.Path.GetType().ToString();
            QueueProperties.Add(pP);

            Property pRDD = new Property();
            pRDD.Name = "requiresduplicatedetection";
            pRDD.MetaData.DisplayName = "Requires Duplicate Detection";
            pRDD.SoType = SoType.YesNo;
            //pRDD.Type = Queue.RequiresDuplicateDetection.GetType().ToString();
            QueueProperties.Add(pRDD);

            Property pRS = new Property();
            pRS.Name = "requiressession";
            pRS.MetaData.DisplayName = "Requires Session";
            pRS.SoType = SoType.YesNo;
            //pRS.Type = Queue.RequiresSession.GetType().ToString();
            QueueProperties.Add(pRS);

            Property pSIB = new Property();
            pSIB.Name = "sizeinbytes";
            pSIB.MetaData.DisplayName = "Size In Bytes";
            pSIB.SoType = SoType.Number;
            //pSIB.Type = Queue.SizeInBytes.GetType().ToString();
            QueueProperties.Add(pSIB);

            QueueProperties.AddRange(ASBStandard.GetStandardReturnProperties());

            return QueueProperties;
        }

        private Method CreateLoadQueue(List<Property> QueueProps)
        {
            Method LoadQueue = new Method();
            LoadQueue.Name = "loadqueue";
            LoadQueue.MetaData.DisplayName = "Load Queue";
            LoadQueue.Type = MethodType.Read;
            LoadQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "queue").First());
            LoadQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "queue").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                LoadQueue.InputProperties.Add(prop);
            }

            foreach (Property prop in QueueProps)
            {
                LoadQueue.ReturnProperties.Add(prop);
            }
            return LoadQueue;
        }

        private Method CreateCreateQueueFullDetails(List<Property> QueueProps)
        {
            Method CreateQueue = new Method();
            CreateQueue.Name = "createqueuefulldetails";
            CreateQueue.MetaData.DisplayName = "Create Queue Full Details";
            CreateQueue.Type = MethodType.Read;
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "queue").First());
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "defaultmessagetimetolive").First());
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "duplicatedetectionhistorytimewindow").First());
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "lockduration").First());
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "maxsizeinmegabytes").First());
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "enabledeadletteringonmessageexpiration").First());
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "requiresduplicatedetection").First());
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "requiressession").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                CreateQueue.InputProperties.Add(prop);
            }

            CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "queue").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "defaultmessagetimetolive").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "duplicatedetectionhistorytimewindow").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "lockduration").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "maxsizeinmegabytes").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "enabledeadletteringonmessageexpiration").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "requiresduplicatedetection").First());
            //CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "requiressession").First());

            foreach (Property prop in QueueProps)
            {
                CreateQueue.ReturnProperties.Add(prop);
            }
            return CreateQueue;
        }

        private Method CreateCreateQueue(List<Property> QueueProps)
        {
            Method CreateQueue = new Method();
            CreateQueue.Name = "createqueue";
            CreateQueue.MetaData.DisplayName = "Create Queue";
            CreateQueue.Type = MethodType.Read;
            CreateQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "queue").First());
            CreateQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "queue").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                CreateQueue.InputProperties.Add(prop);
            }

            foreach (Property prop in QueueProps)
            {
                CreateQueue.ReturnProperties.Add(prop);
            }
            return CreateQueue;
        }

        private Method CreateDeleteQueue(List<Property> QueueProps)
        {
            Method DeleteQueue = new Method();
            DeleteQueue.Name = "deletequeue";
            DeleteQueue.MetaData.DisplayName = "Delete Queue";
            DeleteQueue.Type = MethodType.Read;
            DeleteQueue.InputProperties.Add(QueueProps.Where(p => p.Name == "queue").First());
            DeleteQueue.Validation.RequiredProperties.Add(QueueProps.Where(p => p.Name == "queue").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                DeleteQueue.InputProperties.Add(prop);
            }

            foreach (Property prop in QueueProps)
            {
                DeleteQueue.ReturnProperties.Add(prop);
            }
            return DeleteQueue;
        }

        private Method CreateListQueues(List<Property> QueueProps)
        {
            Method ListQueues = new Method();
            ListQueues.Name = "listqueues";
            ListQueues.MetaData.DisplayName = "List Queues";
            ListQueues.Type = MethodType.List;

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                ListQueues.InputProperties.Add(prop);
            }

            foreach (Property prop in QueueProps)
            {
                ListQueues.ReturnProperties.Add(prop);
            }
            return ListQueues;
        }

        #endregion Definition

        #region Execution
        
        public void LoadQueue(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            NamespaceManager namespaceManager = null;
            QueueDescription Queue = null;
            try
            {
                string queuepath = string.Empty;
                if (inputs.Length == 0)
                {
                    queuepath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    queuepath = inputs.Where(p => p.Name.Equals("queue")).First().Value.ToString();
                }

                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                Queue = namespaceManager.GetQueue(queuepath);

                foreach (Property prop in returns)
                {
                    switch (prop.Name)
                    {
                        case "defaultmessagetimetolive":
                            prop.Value = Queue.DefaultMessageTimeToLive.TotalSeconds;
                            break;
                        case "duplicatedetectionhistorytimewindow":
                            prop.Value = Queue.DuplicateDetectionHistoryTimeWindow.TotalSeconds;
                            break;
                        case "enablebatchedoperations":
                            prop.Value = Queue.EnableBatchedOperations;
                            break;
                        case "enabledeadletteringonmessageexpiration":
                            prop.Value = Queue.EnableDeadLetteringOnMessageExpiration;
                            break;
                        case "isreadonly":
                            prop.Value = Queue.IsReadOnly;
                            break;
                        case "lockduration":
                            prop.Value = Queue.LockDuration.TotalSeconds;
                            break;
                        case "maxdeliverycount":
                            prop.Value = Queue.MaxDeliveryCount;
                            break;
                        case "maxsizeinmegabytes":
                            prop.Value = Queue.MaxSizeInMegabytes;
                            break;
                        case "messagecount":
                            prop.Value = Queue.MessageCount;
                            break;
                        case "queue":
                            prop.Value = Queue.Path;
                            break;
                        case "requiresduplicatedetection":
                            prop.Value = Queue.RequiresDuplicateDetection;
                            break;
                        case "requiressession":
                            prop.Value = Queue.RequiresSession;
                            break;
                        case "sizeinbytes":
                            prop.Value = Queue.SizeInBytes;
                            break;

                    }
                }
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Queue loaded";
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                namespaceManager = null;
                Queue = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ListQueues(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            NamespaceManager namespaceManager = null;
            System.Data.DataRow dr;
            try
            {
                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                IEnumerable<QueueDescription> Qs = namespaceManager.GetQueues();                

                foreach (QueueDescription Queue in Qs)
                {
                    dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                    foreach (Property prop in returns)
                    {
                        switch (prop.Name)
                        {
                            case "defaultmessagetimetolive":
                                dr[prop.Name] = Queue.DefaultMessageTimeToLive.TotalSeconds;
                                break;
                            case "duplicatedetectionhistorytimewindow":
                                dr[prop.Name] = Queue.DuplicateDetectionHistoryTimeWindow.TotalSeconds;
                                break;
                            case "enablebatchedoperations":
                                prop.Value = Queue.EnableBatchedOperations;
                                break;
                            case "enabledeadletteringonmessageexpiration":
                                dr[prop.Name] = Queue.EnableDeadLetteringOnMessageExpiration;
                                break;
                            case "isreadonly":
                                dr[prop.Name] = Queue.IsReadOnly;
                                break;
                            case "lockduration":
                                dr[prop.Name] = Queue.LockDuration.TotalSeconds;
                                break;
                            case "maxdeliverycount":
                                dr[prop.Name] = Queue.MaxDeliveryCount;
                                break;
                            case "maxsizeinmegabytes":
                                dr[prop.Name] = Queue.MaxSizeInMegabytes;
                                break;
                            case "messagecount":
                                dr[prop.Name] = Queue.MessageCount;
                                break;
                            case "queue":
                                dr[prop.Name] = Queue.Path;
                                break;
                            case "requiresduplicatedetection":
                                dr[prop.Name] = Queue.RequiresDuplicateDetection;
                                break;
                            case "requiressession":
                                dr[prop.Name] = Queue.RequiresSession;
                                break;
                            case "sizeinbytes":
                                dr[prop.Name] = Queue.SizeInBytes;
                                break;
                        }
                    }
                    dr["responsestatus"] = ResponseStatus.Success;
                    dr["responsestatusdescription"] = "Queues listed";
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

        public void CreateQueues(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();

            NamespaceManager namespaceManager = null;
            QueueDescription ReturnQueue = null;
            try
            {
                string queuepath = string.Empty;
                if (inputs.Length == 0)
                {
                    queuepath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    queuepath = inputs.Where(p => p.Name.Equals("queue")).First().Value.ToString();
                }

                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());

                if (serviceObject.Methods[0].Name.Equals("createqueuefulldetails"))
                {
                    QueueDescription Queue = new QueueDescription(queuepath);

                    foreach (Property prop in inputs)
                    {
                        if (prop.Value != null)
                        {
                            switch (prop.Name)
                            {
                                case "queue":
                                    Queue.Path = queuepath;
                                    break;
                                case "defaultmessagetimetolive":
                                    Queue.DefaultMessageTimeToLive = new TimeSpan(long.Parse(prop.Value.ToString()) * TimeSpan.TicksPerSecond);
                                    break;
                                case "duplicatedetectionhistorytimewindow":
                                    Queue.DuplicateDetectionHistoryTimeWindow = new TimeSpan(long.Parse(prop.Value.ToString()) * TimeSpan.TicksPerSecond);
                                    break;
                                case "lockduration":
                                    Queue.LockDuration = new TimeSpan(long.Parse(prop.Value.ToString()) * TimeSpan.TicksPerSecond);
                                    break;
                                case "maxsizeinmegabytes":
                                    Queue.MaxSizeInMegabytes = long.Parse(prop.Value.ToString());
                                    break;
                                case "enabledeadletteringonmessageexpiration":
                                    Queue.EnableDeadLetteringOnMessageExpiration = bool.Parse(prop.Value.ToString());
                                    break;
                                case "requiresduplicatedetection":
                                    Queue.RequiresDuplicateDetection = bool.Parse(prop.Value.ToString());
                                    break;
                                case "requiressession":
                                    Queue.RequiresSession = bool.Parse(prop.Value.ToString());
                                    break;
                            }
                        }
                    }

                    ReturnQueue = namespaceManager.CreateQueue(Queue);
                }
                else
                {
                    // create with defaults
                    ReturnQueue = namespaceManager.CreateQueue(queuepath);
                }

                foreach (Property prop in returns)
                {
                    switch (prop.Name)
                    {
                        case "defaultmessagetimetolive":
                            prop.Value = ReturnQueue.DefaultMessageTimeToLive.TotalSeconds;
                            break;
                        case "duplicatedetectionhistorytimewindow":
                            prop.Value = ReturnQueue.DuplicateDetectionHistoryTimeWindow.TotalSeconds;
                            break;
                        case "enablebatchedoperations":
                            prop.Value = ReturnQueue.EnableBatchedOperations;
                            break;
                        case "enabledeadletteringonmessageexpiration":
                            prop.Value = ReturnQueue.EnableDeadLetteringOnMessageExpiration;
                            break;
                        case "isreadonly":
                            prop.Value = ReturnQueue.IsReadOnly;
                            break;
                        case "lockduration":
                            prop.Value = ReturnQueue.LockDuration.TotalSeconds;
                            break;
                        case "maxdeliverycount":
                            prop.Value = ReturnQueue.MaxDeliveryCount;
                            break;
                        case "maxsizeinmegabytes":
                            prop.Value = ReturnQueue.MaxSizeInMegabytes;
                            break;
                        case "messagecount":
                            prop.Value = ReturnQueue.MessageCount;
                            break;
                        case "queue":
                            prop.Value = ReturnQueue.Path;
                            break;
                        case "requiresduplicatedetection":
                            prop.Value = ReturnQueue.RequiresDuplicateDetection;
                            break;
                        case "requiressession":
                            prop.Value = ReturnQueue.RequiresSession;
                            break;
                        case "sizeinbytes":
                            prop.Value = ReturnQueue.SizeInBytes;
                            break;
                    }
                }
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Queue created";

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

        public void DeleteQueues(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            NamespaceManager namespaceManager = null;
            try
            {
                string queuepath = string.Empty;
                if (inputs.Length == 0)
                {
                    queuepath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    queuepath = inputs.Where(p => p.Name.Equals("queue")).First().Value.ToString();
                }

                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                namespaceManager.DeleteQueue(queuepath);
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Queue deleted";

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
