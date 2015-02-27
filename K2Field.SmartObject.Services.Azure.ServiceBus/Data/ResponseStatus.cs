using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace K2Field.SmartObject.Services.Azure.ServiceBus.Data
{
    public class ResponseStatus
    {
        public static readonly string Timeout = "Timeout";
        public static readonly string Error = "Error";
        public static readonly string Sent = "Sent";
        public static readonly string Received = "Received";
        public static readonly string PeekLock = "PeekLock";
        public static readonly string Deferred = "Deferred";
        public static readonly string MessageNotFound = "Message Not Found";
        public static readonly string Success = "Success";
        public static readonly string Abandoned = "Abandoned";
    }
}
