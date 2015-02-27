using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SourceCode.SmartObjects.Services.ServiceSDK;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;

using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;


namespace K2Field.SmartObject.Services.Azure.ServiceBus.Data
{
    public class ASBTopic
    {
        private ServiceAssemblyBase serviceBroker = null;

        public ASBTopic(ServiceAssemblyBase serviceBroker)
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
            List<Property> TopicProps = GetTopicProperties();
            ServiceObject TopicServiceObject = new ServiceObject();
            TopicServiceObject.Name = "azuretopic";
            TopicServiceObject.MetaData.DisplayName = "Azure Topic";
            TopicServiceObject.MetaData.ServiceProperties.Add("objecttype", "topic");
            TopicServiceObject.MetaData.ServiceProperties.Add("objectdiscoverytype", "generictopic");
            TopicServiceObject.Active = true;

            foreach (Property prop in TopicProps)
            {
                TopicServiceObject.Properties.Add(prop);
            }
            TopicServiceObject.Methods.Add(CreateLoadTopic(TopicProps));
            TopicServiceObject.Methods.Add(CreateListTopics(TopicProps));

            serviceBroker.Service.ServiceObjects.Add(TopicServiceObject);
            if (!string.IsNullOrEmpty(folder))
            {
                serviceBroker.Service.ServiceFolders[folder].Add(TopicServiceObject);
            }
        }

        private List<Property> GetTopicProperties()
        {
            List<Property> TopicProperties = new List<Property>();

            Property pDMTTL = new Property();
            pDMTTL.Name = "defaultmessagetimetolive";
            pDMTTL.MetaData.DisplayName = "Default Message Time To Live";
            pDMTTL.SoType = SoType.Decimal;
            TopicProperties.Add(pDMTTL);

            Property pDDHTW = new Property();
            pDDHTW.Name = "duplicatedetectionhistorytimewindow";
            pDDHTW.MetaData.DisplayName = "Duplicate Detection History Time Window";
            pDDHTW.SoType = SoType.Decimal;
            TopicProperties.Add(pDDHTW);

            Property pEBO = new Property();
            pEBO.Name = "enablebatchedoperations";
            pEBO.MetaData.DisplayName = "Enable Batched Operations";
            pEBO.SoType = SoType.YesNo;
            TopicProperties.Add(pEBO);

            Property pIRO = new Property();
            pIRO.Name = "isreadonly";
            pIRO.MetaData.DisplayName = "Is Read Only";
            pIRO.SoType = SoType.YesNo;
            TopicProperties.Add(pIRO);

            Property pMM = new Property();
            pMM.Name = "maxsizeinmegabytes";
            pMM.MetaData.DisplayName = "Max Size In Megabytes";
            pMM.SoType = SoType.Number;
            TopicProperties.Add(pMM);

            Property pP = new Property();
            pP.Name = "topic";
            pP.MetaData.DisplayName = "Topic";
            pP.SoType = SoType.Text;
            TopicProperties.Add(pP);

            Property pRDD = new Property();
            pRDD.Name = "requiresduplicatedetection";
            pRDD.MetaData.DisplayName = "Requires Duplicate Detection";
            pRDD.SoType = SoType.YesNo;
            TopicProperties.Add(pRDD);

            Property pSIB = new Property();
            pSIB.Name = "sizeinbytes";
            pSIB.MetaData.DisplayName = "Size In Bytes";
            pSIB.SoType = SoType.Number;
            TopicProperties.Add(pSIB);

            TopicProperties.AddRange(ASBStandard.GetStandardReturnProperties());

            return TopicProperties;
        }

        private Method CreateLoadTopic(List<Property> TopicProps)
        {
            Method LoadTopic = new Method();
            LoadTopic.Name = "loadtopic";
            LoadTopic.MetaData.DisplayName = "Load Topic";
            LoadTopic.Type = MethodType.Read;
            LoadTopic.InputProperties.Add(TopicProps.Where(p => p.Name == "topic").First());
            LoadTopic.Validation.RequiredProperties.Add(TopicProps.Where(p => p.Name == "topic").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                LoadTopic.InputProperties.Add(prop);
            }

            foreach (Property prop in TopicProps)
            {
                LoadTopic.ReturnProperties.Add(prop);
            }
            return LoadTopic;
        }

        private Method CreateListTopics(List<Property> TopicProps)
        {
            Method ListTopics = new Method();
            ListTopics.Name = "listtopics";
            ListTopics.MetaData.DisplayName = "List Topics";
            ListTopics.Type = MethodType.List;

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                ListTopics.InputProperties.Add(prop);
            }

            foreach (Property prop in TopicProps)
            {
                ListTopics.ReturnProperties.Add(prop);
            }
            return ListTopics;
        }

        #endregion Definition

        #region Execution
        
        public void LoadTopic(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            NamespaceManager namespaceManager = null;
            TopicDescription Topic = null;
            try
            {
                string topicpath = string.Empty;
                if (inputs.Length == 0)
                {
                    topicpath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    topicpath = inputs.Where(p => p.Name.Equals("topic")).First().Value.ToString();
                }

                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                Topic = namespaceManager.GetTopic(topicpath);

                foreach (Property prop in returns)
                {
                    switch (prop.Name)
                    {
                        case "defaultmessagetimetolive":
                            prop.Value = Topic.DefaultMessageTimeToLive.TotalSeconds;
                            break;
                        case "duplicatedetectionhistorytimewindow":
                            prop.Value = Topic.DuplicateDetectionHistoryTimeWindow.TotalSeconds;
                            break;
                        case "enablebatchedoperations":
                            prop.Value = Topic.EnableBatchedOperations;
                            break;
                        case "isreadonly":
                            prop.Value = Topic.IsReadOnly;
                            break;
                        case "maxsizeinmegabytes":
                            prop.Value = Topic.MaxSizeInMegabytes;
                            break;
                        case "topic":
                            prop.Value = Topic.Path;
                            break;
                        case "requiresduplicatedetection":
                            prop.Value = Topic.RequiresDuplicateDetection;
                            break;
                        case "sizeinbytes":
                            prop.Value = Topic.SizeInBytes;
                            break;

                    }
                    returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                    returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Topic loaded";
                }
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                namespaceManager = null;
                Topic = null;
            }

            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ListTopics(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            System.Data.DataRow dr;
            NamespaceManager namespaceManager = null;
            try
            {
                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                IEnumerable<TopicDescription> Ts = namespaceManager.GetTopics();

                foreach (TopicDescription Topic in Ts)
                {
                    dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                    foreach (Property prop in returns)
                    {
                        switch (prop.Name)
                        {
                            case "defaultmessagetimetolive":
                                dr[prop.Name] = Topic.DefaultMessageTimeToLive.TotalSeconds;
                                break;
                            case "duplicatedetectionhistorytimewindow":
                                dr[prop.Name] = Topic.DuplicateDetectionHistoryTimeWindow.TotalSeconds;
                                break;
                            case "enablebatchedoperations":
                                prop.Value = Topic.EnableBatchedOperations;
                                break;
                            case "isreadonly":
                                dr[prop.Name] = Topic.IsReadOnly;
                                break;
                            case "maxsizeinmegabytes":
                                dr[prop.Name] = Topic.MaxSizeInMegabytes;
                                break;
                            case "topic":
                                dr[prop.Name] = Topic.Path;
                                break;
                            case "requiresduplicatedetection":
                                dr[prop.Name] = Topic.RequiresDuplicateDetection;
                                break;
                            case "sizeinbytes":
                                dr[prop.Name] = Topic.SizeInBytes;
                                break;
                        }
                    }
                    dr["responsestatus"] = ResponseStatus.Success;
                    dr["responsestatusdescription"] = "Topics listed";
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

        #endregion Execution
    }
}
