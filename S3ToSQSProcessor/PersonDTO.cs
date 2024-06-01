using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace S3ToSQSProcessor
{
    public class PersonDTO
    {
        public int SNo { get; set; }

        public string AccountNumber { get; set; }

        public string FirstName { get; set; }

        public string LastName { get; set; }
    }

    public class PersonMap : ClassMap<PersonDTO>
    {
        public PersonMap()
        {
            Map(p => p.SNo).Index(0);
            Map(p => p.AccountNumber).Index(1);
            Map(p => p.FirstName).Index(2);
            Map(p => p.LastName).Index(3);
        }
    }
}
