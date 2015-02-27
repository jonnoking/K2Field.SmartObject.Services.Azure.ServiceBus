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
    public class ASBSubscription
    {
        private ServiceAssemblyBase serviceBroker = null;

        public ASBSubscription(ServiceAssemblyBase serviceBroker)
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
            List<Property> SubscriptionProps = GetSubscriptionProperties();
            ServiceObject SubscriptionServiceObject = new ServiceObject();
            SubscriptionServiceObject.Name = "azuresubscription";
            SubscriptionServiceObject.MetaData.DisplayName = "Azure Subscription";
            SubscriptionServiceObject.MetaData.ServiceProperties.Add("objecttype", "subscription");
            SubscriptionServiceObject.MetaData.ServiceProperties.Add("objectdiscoverytype", "genericsubscription");
            SubscriptionServiceObject.Active = true;

            foreach (Property prop in SubscriptionProps)
            {
                SubscriptionServiceObject.Properties.Add(prop);
            }
            SubscriptionServiceObject.Methods.Add(CreateLoadSubscription(SubscriptionProps));
            SubscriptionServiceObject.Methods.Add(CreateListSubscriptions(SubscriptionProps));

            serviceBroker.Service.ServiceObjects.Add(SubscriptionServiceObject);

            if (!string.IsNullOrEmpty(folder))
            {
                serviceBroker.Service.ServiceFolders[folder].Add(SubscriptionServiceObject);
            }
        }

        private List<Property> GetSubscriptionProperties()
        {
            List<Property> SubscriptionProperties = new List<Property>();

            Property pDMTTL = new Property();
            pDMTTL.Name = "defaultmessagetimetolive";
            pDMTTL.MetaData.DisplayName = "Default Message Time To Live";
            pDMTTL.SoType = SoType.Decimal;
            SubscriptionProperties.Add(pDMTTL);

            Property pEBO = new Property();
            pEBO.Name = "enablebatchedoperations";
            pEBO.MetaData.DisplayName = "Enable Batched Operations";
            pEBO.SoType = SoType.YesNo;
            //pEBO.Type = Queue.EnableBatchedOperations.GetType().ToString();
            SubscriptionProperties.Add(pEBO);

            Property pEDLFE = new Property();
            pEDLFE.Name = "enabledeadletteringonfilterevaluationexceptions";
            pEDLFE.MetaData.DisplayName = "Enable Dead Lettering On Filter Evaluation Exceptions";
            pEDLFE.SoType = SoType.YesNo;
            SubscriptionProperties.Add(pEDLFE);

            Property pEDL = new Property();
            pEDL.Name = "enabledeadletteringonmessageexpiration";
            pEDL.MetaData.DisplayName = "Enable Dead Lettering On Message Expiration";
            pEDL.SoType = SoType.YesNo;
            SubscriptionProperties.Add(pEDL);

            Property pIRO = new Property();
            pIRO.Name = "isreadonly";
            pIRO.MetaData.DisplayName = "Is Read Only";
            pIRO.SoType = SoType.YesNo;
            SubscriptionProperties.Add(pIRO);

            Property pLD = new Property();
            pLD.Name = "lockduration";
            pLD.MetaData.DisplayName = "Lock Duration";
            pLD.SoType = SoType.Decimal;
            SubscriptionProperties.Add(pLD);

            Property pMDC = new Property();
            pMDC.Name = "maxdeliverycount";
            pMDC.MetaData.DisplayName = "Max Delivery Count";
            pMDC.SoType = SoType.Number;
            SubscriptionProperties.Add(pMDC);

            Property pMC = new Property();
            pMC.Name = "messagecount";
            pMC.MetaData.DisplayName = "Message Count";
            pMC.SoType = SoType.Number;
            SubscriptionProperties.Add(pMC);

            Property pS = new Property();
            pS.Name = "subscription";
            pS.MetaData.DisplayName = "Subscription";
            pS.SoType = SoType.Text;
            SubscriptionProperties.Add(pS);

            Property pRS = new Property();
            pRS.Name = "requiressession";
            pRS.MetaData.DisplayName = "Requires Session";
            pRS.SoType = SoType.YesNo;
            SubscriptionProperties.Add(pRS);

            Property pP = new Property();
            pP.Name = "topic";
            pP.MetaData.DisplayName = "Topic";
            pP.SoType = SoType.Text;
            SubscriptionProperties.Add(pP);

            SubscriptionProperties.AddRange(ASBStandard.GetStandardReturnProperties());

            return SubscriptionProperties;
        }

        private Method CreateLoadSubscription(List<Property> SubscriptionProps)
        {
            Method LoadSubscription = new Method();
            LoadSubscription.Name = "loadsubscription";
            LoadSubscription.MetaData.DisplayName = "Load Subscription";
            LoadSubscription.Type = MethodType.Read;
            LoadSubscription.InputProperties.Add(SubscriptionProps.Where(p => p.Name == "subscription").First());
            LoadSubscription.Validation.RequiredProperties.Add(SubscriptionProps.Where(p => p.Name == "subscription").First());
            LoadSubscription.InputProperties.Add(SubscriptionProps.Where(p => p.Name == "topic").First());
            LoadSubscription.Validation.RequiredProperties.Add(SubscriptionProps.Where(p => p.Name == "topic").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                LoadSubscription.InputProperties.Add(prop);
            }

            foreach (Property prop in SubscriptionProps)
            {
                LoadSubscription.ReturnProperties.Add(prop);
            }
            return LoadSubscription;
        }

        private Method CreateListSubscriptions(List<Property> SubscriptionProps)
        {
            Method ListSubscriptions = new Method();
            ListSubscriptions.Name = "listsubscriptions";
            ListSubscriptions.MetaData.DisplayName = "List Subscriptions";
            ListSubscriptions.Type = MethodType.List;
            ListSubscriptions.InputProperties.Add(SubscriptionProps.Where(p => p.Name == "topic").First());
            ListSubscriptions.Validation.RequiredProperties.Add(SubscriptionProps.Where(p => p.Name == "topic").First());

            foreach (Property prop in ASBStandard.GetStandardInputProperties())
            {
                ListSubscriptions.InputProperties.Add(prop);
            }


            foreach (Property prop in SubscriptionProps)
            {
                ListSubscriptions.ReturnProperties.Add(prop);
            }


            return ListSubscriptions;
        }

        #endregion Definition

        #region Exectution

        public void LoadSubscription(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            NamespaceManager namespaceManager = null;
            SubscriptionDescription Subscription = null;

            try
            {
                string subscriptionpath = string.Empty;
                if (inputs.Length == 1)
                {
                    subscriptionpath = serviceObject.MetaData.DisplayName;
                }
                else
                {
                    subscriptionpath = inputs.Where(p => p.Name.Equals("subscription")).First().Value.ToString();
                }

                string topicpath = string.Empty;
                topicpath = inputs.Where(p => p.Name.Equals("topic")).First().Value.ToString();

                namespaceManager = serviceUtilities.GetNamespaceManager(inputs.Where(p => p.Name.Equals("requesttimeout")).FirstOrDefault());
                Subscription = namespaceManager.GetSubscription(topicpath, subscriptionpath);

                foreach (Property prop in returns)
                {
                    switch (prop.Name)
                    {
                        case "defaultmessagetimetolive":
                            prop.Value = Subscription.DefaultMessageTimeToLive.TotalSeconds;
                            break;
                        case "enablebatchedoperations":
                            prop.Value = Subscription.EnableBatchedOperations;
                            break;
                        case "enabledeadletteringonfilterevaluationexceptions":
                            prop.Value = Subscription.EnableDeadLetteringOnFilterEvaluationExceptions;
                            break;
                        case "enabledeadletteringonmessageexpiration":
                            prop.Value = Subscription.EnableDeadLetteringOnMessageExpiration;
                            break;
                        case "isreadonly":
                            prop.Value = Subscription.IsReadOnly;
                            break;
                        case "lockduration":
                            prop.Value = Subscription.LockDuration.TotalSeconds;
                            break;
                        case "maxdeliverycount":
                            prop.Value = Subscription.MaxDeliveryCount;
                            break;
                        case "messagecount":
                            prop.Value = Subscription.MessageCount;
                            break;
                        case "subscription":
                            prop.Value = Subscription.Name;
                            break;
                        case "requiressession":
                            prop.Value = Subscription.RequiresSession;
                            break;
                        case "topic":
                            prop.Value = Subscription.TopicPath;
                            break;
                    }
                }
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Success;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = "Subscription loaded";
            }
            catch (Exception ex)
            {
                returns.Where(p => p.Name.Equals("responsestatus")).First().Value = ResponseStatus.Error;
                returns.Where(p => p.Name.Equals("responsestatusdescription")).First().Value = ex.Message;
            }
            finally
            {
                namespaceManager = null;
                Subscription = null;
            }
            serviceObject.Properties.BindPropertiesToResultTable();
        }

        public void ListSubscriptions(Property[] inputs, RequiredProperties required, Property[] returns, MethodType methodType, ServiceObject serviceObject)
        {
            Utilities.ServiceUtilities serviceUtilities = new Utilities.ServiceUtilities(serviceBroker);
            serviceObject.Properties.InitResultTable();
            System.Data.DataRow dr;
            NamespaceManager namespaceManager = null;
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
                IEnumerable<SubscriptionDescription> Ss = namespaceManager.GetSubscriptions(topicpath);

                foreach (SubscriptionDescription Subscription in Ss)
                {
                    dr = serviceBroker.ServicePackage.ResultTable.NewRow();
                    foreach (Property prop in returns)
                    {
                        switch (prop.Name)
                        {
                            case "defaultmessagetimetolive":
                                dr[prop.Name] = Subscription.DefaultMessageTimeToLive.TotalSeconds;
                                break;
                            case "enablebatchedoperations":
                                dr[prop.Name] = Subscription.EnableBatchedOperations;
                                break;
                            case "enabledeadletteringonfilterevaluationexceptions":
                                dr[prop.Name] = Subscription.EnableDeadLetteringOnFilterEvaluationExceptions;
                                break;
                            case "enabledeadletteringonmessageexpiration":
                                dr[prop.Name] = Subscription.EnableDeadLetteringOnMessageExpiration;
                                break;
                            case "isreadonly":
                                dr[prop.Name] = Subscription.IsReadOnly;
                                break;
                            case "lockduration":
                                dr[prop.Name] = Subscription.LockDuration.TotalSeconds;
                                break;
                            case "maxdeliverycount":
                                dr[prop.Name] = Subscription.MaxDeliveryCount;
                                break;
                            case "messagecount":
                                dr[prop.Name] = Subscription.MessageCount;
                                break;
                            case "subscription":
                                dr[prop.Name] = Subscription.Name;
                                break;
                            case "requiressession":
                                dr[prop.Name] = Subscription.RequiresSession;
                                break;
                            case "topic":
                                dr[prop.Name] = Subscription.TopicPath;
                                break;
                        }
                    }
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

        #endregion Exectution
    }
}
