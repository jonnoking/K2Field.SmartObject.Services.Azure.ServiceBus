using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SourceCode.SmartObjects.Services.ServiceSDK.Objects;
using SourceCode.SmartObjects.Services.ServiceSDK.Types;

namespace K2Field.SmartObject.Services.Azure.ServiceBus.Data
{
    static class ASBStandard
    {
        public static List<Property> GetStandardInputProperties()
        {
            List<Property> StandardInputProperties = new List<Property>();

            Property p18 = new Property();
            p18.Name = "requesttimeout";
            p18.MetaData.DisplayName = "Request Timeout";
            p18.SoType = SoType.Number;
            StandardInputProperties.Add(p18);

            return StandardInputProperties;
        }

        public static List<Property> GetStandardReturnProperties()
        {
            List<Property> StandardReturnProperties = new List<Property>();

            StandardReturnProperties.AddRange(GetStandardInputProperties());

            Property p19 = new Property();
            p19.Name = "responsestatus";
            p19.MetaData.DisplayName = "Response Status";
            p19.SoType = SoType.Text;
            StandardReturnProperties.Add(p19);

            Property p20 = new Property();
            p20.Name = "responsestatusdescription";
            p20.MetaData.DisplayName = "Response Status Description";
            p20.SoType = SoType.Text;
            StandardReturnProperties.Add(p20);

            return StandardReturnProperties;

        }
    }
}
