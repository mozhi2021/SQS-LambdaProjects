using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqsToDynamoDB
{
    public class PersonDTO
    {
        public int SNo { get; set; }

        public string AccountNumber { get; set; }

        public string FirstName { get; set; }

        public string LastName { get; set; }
    }
}
