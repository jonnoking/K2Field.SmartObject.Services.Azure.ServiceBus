using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using SourceCode.SmartObjects.Services.ServiceSDK;
using K2Field.SmartObject.Services.Azure.ServiceBus.Data;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;

namespace K2Field.SmartObject.Services.Azure.ServiceBus.Utilities
{
    public class ServiceUtilities
    {
        private ServiceAssemblyBase serviceBroker = null;       

        public ServiceUtilities(ServiceAssemblyBase serviceBroker)
        {
            // Set local serviceBroker variable.
            this.serviceBroker = serviceBroker;
        }        
        public MessagingFactory GetMessagingFactory(long timeoutSeconds)
        {
            try
            {
                MessagingFactorySettings mfSettings = new MessagingFactorySettings();
                mfSettings.TokenProvider = GetTokenProvider();
                mfSettings.OperationTimeout = GetRequestTimeout(timeoutSeconds);

                return MessagingFactory.Create(GetServiceUri(), mfSettings);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public MessagingFactory GetMessagingFactory(Property requestTimeout)
        {
            if (requestTimeout.Value != null)
            {
                long timeout = -1;
                if (long.TryParse(requestTimeout.Value.ToString(), out timeout))
                {
                    return GetMessagingFactory(timeout);
                }
                else
                {
                    return GetMessagingFactory(-1);
                }
            }
            else
            {
                return GetMessagingFactory(-1);
            }
        }

        public NamespaceManager GetNamespaceManager(Property requestTimeout)
        {
            if (requestTimeout.Value != null)
            {
                long timeout = -1;
                if (long.TryParse(requestTimeout.Value.ToString(), out timeout))
                {
                    return GetNamespaceManager(timeout);
                }
                else
                {
                    return GetNamespaceManager(-1);
                }
            }
            else
            {
                return GetNamespaceManager(-1);
            }
        }

        public NamespaceManager GetNamespaceManager(long timeoutSeconds)
        {
            NamespaceManagerSettings nmSettings = new NamespaceManagerSettings();
            nmSettings.TokenProvider = GetTokenProvider();
            nmSettings.OperationTimeout = GetRequestTimeout(timeoutSeconds);
            return new NamespaceManager(GetServiceUri(), nmSettings);
        }

        public TimeSpan GetRequestTimeout(long timeoutSeconds)
        {
            if (timeoutSeconds > 0)
            {
                return new TimeSpan(timeoutSeconds * TimeSpan.TicksPerSecond);
            }
            else
            {
                return new TimeSpan(long.Parse(serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.RequestTimeout].ToString()) * TimeSpan.TicksPerSecond);
            }
        }

        public MessageReceiver GetMessageReceiver(MessagingFactory factory, string queueSubscription)
        {
            try
            {
                return factory.CreateMessageReceiver(queueSubscription);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public TokenProvider GetTokenProvider()
        {
            TokenProvider tp = null;
            try
            {
                return TokenProvider.CreateSharedSecretTokenProvider(serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.Issuer].ToString(), serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.Key].ToString());
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public Uri GetServiceUri()
        {
            return ServiceBusEnvironment.CreateServiceUri("sb", serviceBroker.Service.ServiceConfiguration[ServiceConfigurationSettings.Namespace].ToString(), string.Empty);
        }
    }
}
