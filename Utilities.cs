using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections.Generic;
using System.Collections;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Text;
using System.Xml;

    #region AtMax etc

#if SQL2008
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = -1)]
#else
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = 8000)]
#endif
public struct ATMAX : IBinarySerialize
{
    private object max;
    private object item;
    private bool m_null;

    public void Init()
    {
        max = null;
        item = 0.0;
        m_null = true;
    }

#if SQL2008
    public void Accumulate(object o1, object o2)
    {
#else
    public void Accumulate(tuple_2 input)
    {
        if (input.IsNull || input.v1IsNull) return;
        object o1 = input.v1;
        object o2 = input.v2;
#endif
            if (Utility.IsNull(o1) || Utility.IsNull(o2)) return;
            if (m_null || ((IComparable)Utility.GetValue(o1)).CompareTo(max) > 0)
            {
                max = Utility.GetValue(o1);
                item = Utility.GetValue(o2);
                m_null = false;
            }
    }

    public void Merge(ATMAX Group)
    {
        if (!Group.m_null)
        {
            if (m_null || ((IComparable)Group.max).CompareTo(max) > 0)
            {
                max = Group.max;
                item = Group.item;
            }
            m_null = false;
        }
    }

    public object Terminate()
    {
        if (m_null) return SqlDouble.Null;
        return item;
    }

    public void Write(System.IO.BinaryWriter w)
    {
        Utility.write(w, max);
        Utility.write(w, item);
        w.Write(m_null);
    }

    public void Read(System.IO.BinaryReader r)
    {
        max = Utility.read(r);
        item = Utility.read(r);
        m_null = r.ReadBoolean();
    }
}

#if SQL2008
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = -1)]
#else
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = 8000)]
#endif
public struct ATMIN : IBinarySerialize
{
    private object min;
    private object item;
    private bool m_null;

    public void Init()
    {
        min = null;
        item = 0.0;
        m_null = true;
    }

#if SQL2008
    public void Accumulate(object o1, object o2)
    {
#else
    public void Accumulate(tuple_2 input)
    {
        if (input.IsNull || input.v1IsNull) return;
        object o1 = input.v1;
        object o2 = input.v2;
#endif
        if (o1 == DBNull.Value) return;
        if (m_null || ((IComparable)Utility.GetValue(o1)).CompareTo(min) < 0)
        {
            min = Utility.GetValue(o1);
            item = Utility.GetValue(o2);
            m_null = false;
        }
    }

    public void Merge(ATMIN Group)
    {
        if (!Group.m_null)
        {
            if (m_null || ((IComparable)Group.min).CompareTo(min) < 0)
            {
                min = Group.min;
                item = Group.item;
            }
            m_null = false;
        }
    }

    public object Terminate()
    {
        if (m_null) return SqlDouble.Null;
        return item;
    }

    public void Write(System.IO.BinaryWriter w)
    {
        Utility.write(w, min);
        Utility.write(w, item);
        w.Write(m_null);
    }

    public void Read(System.IO.BinaryReader r)
    {
        min = Utility.read(r);
        item = Utility.read(r);
        m_null = r.ReadBoolean();
    }
}

#if SQL2008
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = -1)]
#else
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = 8000)]
#endif
public struct ATMATCH : IBinarySerialize
{
    private object item;
    private object match;
    private bool m_null;

    public void Init()
    {
        item = null;
        match = null;
        m_null = true;
    }

#if SQL2008
    public void Accumulate(object o1, object o2, object o3)
    {
#else
    public void Accumulate(tuple_3 input)
    {
        if (input.IsNull || input.v1IsNull || input.v2IsNull) return;
        object o1 = input.v1;
        object o2 = input.v2;
        object o3 = input.v3;
#endif
        if (o1 == DBNull.Value) return;
        if (o2 == DBNull.Value) return;
        if (m_null && ((IComparable)Utility.GetValue(o1)).CompareTo((IComparable)Utility.GetValue(o2)) == 0)
        {
            item = Utility.GetValue(o3);
            match = Utility.GetValue(o1);
            m_null = false;
        }
    }

    public void Merge(ATMATCH Group)
    {
        if (!Group.m_null)
        {
            if (m_null)
            {
                match = Group.match;
                item = Group.item;
                m_null = false;
            }
        }
    }

    public object Terminate()
    {
        if (m_null) return SqlDouble.Null;
        return item;
    }

    public void Write(System.IO.BinaryWriter w)
    {
        Utility.write(w, match);
        Utility.write(w, item);
        w.Write(m_null);
    }

    public void Read(System.IO.BinaryReader r)
    {
        match = Utility.read(r);
        item = Utility.read(r);
        m_null = r.ReadBoolean();
    }
}

    #endregion


#if SQL2008
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = -1)]
#else
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = 8000)]
#endif
    public struct VCOALESCE : IBinarySerialize
    {
        private int min;
        private object item;
        private bool m_null;

        public void Init()
        {
            min = int.MaxValue;
            item = DBNull.Value;
            m_null = true;
        }

#if SQL2008
        public void Accumulate(object o1, int sequence)
        {
#else
    public void Accumulate(tuple_2 input)
    {
        if (input.IsNull || input.v1IsNull) return;
        object o1 = input.v1;
        int sequence = input.v2;
#endif
            if (o1 == DBNull.Value) return;
            if (m_null || sequence < min)
            {
                min = sequence;
                item = Utility.GetValue(o1);
                m_null = false;
            }
        }

        public void Merge(VCOALESCE Group)
        {
            if (!Group.m_null)
            {
                if (m_null || Group.min < min)
                {
                    min = Group.min;
                    item = Group.item;
                }
                m_null = false;
            }
        }

        public object Terminate()
        {
            if (m_null) return DBNull.Value;
            return item;
        }

        public void Write(System.IO.BinaryWriter w)
        {
            w.Write(min);
            Utility.write(w, item);
            w.Write(m_null);
        }

        public void Read(System.IO.BinaryReader r)
        {
            min = r.ReadInt32();
            item = Utility.read(r);
            m_null = r.ReadBoolean();
        }
    }



#if SQL2008
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = -1)]
#else
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = 8000)]
#endif
    public struct STRCAT : IBinarySerialize
    {
        private List<string> values;
        private string separator;

        public void Init()
        {
            this.values = new List<string>();
            separator = "";
        }

#if SQL2008
        public void Accumulate(SqlChars value, SqlChars sep)
        {
#else
        public void Accumulate(tuple_2 input)
        {
            if (input.IsNull || input.v1IsNull) return;
            SqlChars value = new SqlChars((SqlString)input.v1);
            SqlChars sep = input.v2IsNull ? new SqlChars("") : new SqlChars((SqlString)input.v2);
#endif
            if (value.IsNull) return;
            string s = new string(value.Value);
            if (separator == "") separator = sep.IsNull ? "," : new string(sep.Value);
            this.values.Add(s);
        }

        public void Merge(STRCAT value)
        {
            this.values.AddRange(value.values.ToArray());
            if (this.separator == "") this.separator = value.separator;
            else if (this.separator != value.separator && value.separator != "")
                throw new Exception(string.Format("Strcat: separator must be constant (got {0:G} and {1:G}", this.separator, value.separator));
        }

        public SqlChars Terminate()
        {
            string s = string.Join(this.separator, this.values.ToArray());
            return new SqlChars(s);
        }

        public void Write(System.IO.BinaryWriter w)
        {
            w.Write(separator);
            w.Write(values.Count);
            foreach (string s in values) w.Write(s);
        }

        public void Read(System.IO.BinaryReader r)
        {
            separator = r.ReadString();
            int n = r.ReadInt32();
            values = new List<string>();
            for (int i = 0; i < n; i++) values.Add(r.ReadString());
        }
    }

#if SQL2008
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = -1)]
#else
    [SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize = 8000)]
#endif
    public struct STRCATASC : IBinarySerialize
    {
        private List<string> values;
        private string separator;

        public void Init()
        {
            this.values = new List<string>();
            separator = "";
        }

#if SQL2008
        public void Accumulate(SqlChars value, SqlChars sep)
        {
#else
        public void Accumulate(tuple_2 input)
        {
            if (input.IsNull || input.v1IsNull) return;
            SqlChars value = new SqlChars((SqlString)input.v1);
            SqlChars sep = input.v2IsNull ? new SqlChars("") : new SqlChars((SqlString)input.v2);
#endif
            if (value.IsNull) return;
            string s = new string(value.Value);
            if (separator == "") separator = sep.IsNull ? "," : new string(sep.Value);
            else if (!sep.IsNull && separator != new string(sep.Value))
                throw new Exception(string.Format("Strcat: separator must be constant (got {0:G} and {1:G}", this.separator, sep.Value)); 
            this.values.Add(s);
        }

        public void Merge(STRCATASC value)
        {
            this.values.AddRange(value.values.ToArray());
            if (this.separator == "") this.separator = value.separator;
            else if (this.separator != value.separator && value.separator != "")
                throw new Exception(string.Format("Strcat: separator must be constant (got {0:G} and {1:G}", this.separator, value.separator));
        }

        public SqlChars Terminate()
        {
            this.values.Sort();
            string s = string.Join(this.separator, this.values.ToArray());
            return new SqlChars(s);
        }

        public void Write(System.IO.BinaryWriter w)
        {
            w.Write(separator);
            w.Write(values.Count);
            foreach (string s in values) w.Write(s);
        }

        public void Read(System.IO.BinaryReader r)
        {
            separator = r.ReadString();
            int n = r.ReadInt32();
            values = new List<string>();
            for (int i = 0; i < n; i++) values.Add(r.ReadString());
        }
    }


public class Utility
{
    private static readonly List<bool> license = new List<bool>();
    internal static void ValidateKey()
    {
#if TRIAL
        if (license.Count > 0) return;
        using (SqlConnection conn = new SqlConnection("context connection=true"))
        {
            SqlCommand command = new SqlCommand("SELECT SERVERPROPERTY('edition')", conn);
            conn.Open();
            object o = command.ExecuteScalar();
            if (o != null && o.GetType().ToString() == "System.String")
            {
                string s = (string)o;
                if (s.StartsWith("Developer Edition"))
                {
                    license.Add(true);
                    return;
                }
            }

            command.CommandText = "SELECT af.content FROM sys.assemblies a INNER JOIN sys.assembly_files af ON a.assembly_id = af.assembly_id WHERE a.name like 'SQLUtilities%' AND af.name = 'license.key'";
            o = command.ExecuteScalar();
            if (o == null || o.GetType().ToString() != "System.Byte[]") throw new Exception("Error: No valid trial license key found for SQLUtilities. Please visit www.totallysql.com to purchase.");
            byte[] key = (byte[])o;
            if (key.Length != 8) throw new Exception("Error: No valid trial license key found for SQLUtilities. Please visit www.totallysql.com to purchase.");
            ushort a, ticks = 0, prodcust = 0, cust2 = 0;
            for (int j = 3; j >= 0; j--)
            {
                a = BitConverter.ToUInt16(key, j * 2);
                for (int i = 0; i < 4; i++)
                {
                    a = (ushort)(a >> 1);
                    cust2 = (ushort)((cust2 << 1) + (a & 1));
                    a = (ushort)(a >> 1);
                    prodcust = (ushort)((prodcust << 1) + (a & 1));
                    a = (ushort)(a >> 1);
                    ticks = (ushort)((ticks << 1) + (a & 1));
                    a = (ushort)(a >> 1);
                }
            }
            ulong ticktotal = (ulong)new DateTime(2010, 1, 1).Ticks / 864000000000 + (ulong)ticks;
            DateTime d = new DateTime((long)ticktotal * 864000000000);
            byte product = (byte)(prodcust & 0xFF);
            uint customer = cust2;
            customer = (uint)((customer << 8) + (prodcust >> 8));
            if (product != 0x6D) throw new Exception("Error: License key is not valid for this product (SQLUtilities). Please visit www.totallysql.com to purchase.");
            if (d.CompareTo(new DateTime(2010, 1, 6)) == 0)
                throw new Exception("Error - you need to activate the trial license first: EXEC dbo.ActivateSQLUtilitiesTrial ");
            if (d <= DateTime.UtcNow) throw new Exception("Error: trial license for SQLUtilities has expired. Please visit www.totallysql.com to purchase.");
        }
        license.Add(true);
#endif
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime SQLUtilitiesExpires()
    {
#if TRIAL
        object o;

        using (SqlConnection conn = new SqlConnection("context connection=true"))
        {
            SqlCommand command = new SqlCommand("SELECT SERVERPROPERTY('edition')", conn);
            conn.Open();
            o = command.ExecuteScalar();
            if (o != null && o.GetType().ToString() == "System.String")
            {
                string s = (string)o;
                if (s.StartsWith("Developer Edition")) return SqlDateTime.MaxValue;
            }

            command.CommandText = "SELECT af.content FROM sys.assemblies a INNER JOIN sys.assembly_files af ON a.assembly_id = af.assembly_id WHERE a.name like 'SQLUtilities%' AND af.name = 'license.key'";
            o = command.ExecuteScalar();
        }

        if (o == null || o.GetType().ToString() != "System.Byte[]") throw new Exception("Error: No valid trial license key found for SQLUtilities. Please visit www.totallysql.com to purchase.");
        byte[] key = (byte[])o;
        if (key.Length != 8) throw new Exception("Error: No valid trial license key found for SQLUtilities. Please visit www.totallysql.com to purchase.");
        ushort a, ticks = 0, prodcust = 0, cust2 = 0;
        for (int j = 3; j >= 0; j--)
        {
            a = BitConverter.ToUInt16(key, j * 2);
            for (int i = 0; i < 4; i++)
            {
                a = (ushort)(a >> 1);
                cust2 = (ushort)((cust2 << 1) + (a & 1));
                a = (ushort)(a >> 1);
                prodcust = (ushort)((prodcust << 1) + (a & 1));
                a = (ushort)(a >> 1);
                ticks = (ushort)((ticks << 1) + (a & 1));
                a = (ushort)(a >> 1);
            }
        }
        ulong ticktotal = (ulong)new DateTime(2010, 1, 1).Ticks / 864000000000 + (ulong)ticks;
        DateTime d = new DateTime((long)ticktotal * 864000000000);
        byte product = (byte)(prodcust & 0xFF);
        uint customer = cust2;
        customer = (uint)((customer << 8) + (prodcust >> 8));
        if (product != 0x6D) throw new Exception("Error: License key is not valid for this product (SQLUtilities). Please visit www.totallysql.com to purchase.");
        return new SqlDateTime(d);
#else
        return SqlDateTime.MaxValue;
#endif
    }

#if TRIAL
    [SqlProcedure]
    public static void ActivateSQLUtilitiesTrial()
    {
        DateTime expiry = SQLUtilitiesExpires().Value;
        if (expiry.CompareTo(new DateTime(2010, 1, 6)) == 0)
        {
            string key = new_key();
            using (SqlConnection conn = new SqlConnection("context connection=true"))
            {
                SqlCommand command = new SqlCommand("alter assembly [SQLUtilities2005Trial] drop file 'license.key'", conn);
                conn.Open();
                command.ExecuteNonQuery();
                command.CommandText = "ALTER ASSEMBLY [SQLUtilities2005Trial] ADD FILE FROM " + key + " AS 'license.key'";
                command.ExecuteNonQuery();
                conn.Close();
            }
            SqlContext.Pipe.Send("SQLUtilities trial license successfully activated - expires: " + DateTime.Today.AddDays(31).ToShortDateString());
        }
        else
            SqlContext.Pipe.Send("Error: trial activation is not available on this installation. Please remove and install a fresh copy if you wish to extend the trial, or visit www.totallysql.com to purchase a perpetual license.");
    }

    private static string new_key()
    {
        DateTime Expiry = DateTime.Today.AddDays(31);
        ulong tickbase = (ulong)new DateTime(2010, 1, 1).Ticks / 864000000000;
        ushort ticks = (ushort)((ulong)Expiry.Ticks / 864000000000 - tickbase);
        ushort seed = (ushort)rand.Next();
        uint customer = (ushort)rand.Next();
        byte product = 0x6D;
        ushort prodcust = (ushort)((customer << 8) + product);
        ushort cust2 = (ushort)(customer >> 8);
        byte[] key = new byte[8];
        ushort a = 0;
        for (int j = 0; j < 4; j++)
        {
            a = 0;
            for (int i = 0; i < 4; i++)
            {
                a = (ushort)((a << 1) + (ticks & 1)); ticks = (ushort)(ticks >> 1);
                a = (ushort)((a << 1) + (prodcust & 1)); prodcust = (ushort)(prodcust >> 1);
                a = (ushort)((a << 1) + (cust2 & 1)); cust2 = (ushort)(cust2 >> 1);
                a = (ushort)((a << 1) + (seed & 1)); seed = (ushort)(seed >> 1);
            }
            BitConverter.GetBytes(a).CopyTo(key, j * 2);
        }
        string s = "0x" + BitConverter.ToString(key).Replace("-", "");
        return s;
    }
#endif


    internal static bool IsNull(object o)
    {
        if (o == null) return true;
        switch (o.GetType().Name)
        {
            case "SqlBinary": return ((SqlBinary)o).IsNull;
            case "SqlBoolean": return ((SqlBoolean)o).IsNull;
            case "SqlByte": return ((SqlByte)o).IsNull;
            case "SqlBytes": return ((SqlBytes)o).IsNull;
            case "SqlChars": return ((SqlChars)o).IsNull;
            case "SqlDateTime": return ((SqlDateTime)o).IsNull;
            case "SqlDecimal": return ((SqlDecimal)o).IsNull;
            case "SqlDouble": return ((SqlDouble)o).IsNull;
            case "SqlGuid": return ((SqlGuid)o).IsNull;
            case "SqlInt16": return ((SqlInt16)o).IsNull;
            case "SqlInt32": return ((SqlInt32)o).IsNull;
            case "SqlInt64": return ((SqlInt64)o).IsNull;
            case "SqlMoney": return ((SqlMoney)o).IsNull;
            case "SqlSingle": return ((SqlSingle)o).IsNull;
            case "SqlString": return ((SqlString)o).IsNull;
            case "SqlXml": return ((SqlXml)o).IsNull;
            case "DBNull": return true;
            case "DateTime": return false;
            default: throw new Exception("Type not recognised: " + o.GetType().Name);
        }
    }

    internal static object GetValue(object o)
    {
        if (IsNull(o)) return null;
        switch (o.GetType().Name)
        {
            case "SqlBinary": return ((SqlBinary)o).Value;
            case "SqlBoolean": return ((SqlBoolean)o).Value;
            case "SqlByte": return ((SqlByte)o).Value;
            case "SqlBytes": return ((SqlBytes)o).Value;
            case "SqlChars": return ((SqlChars)o).Value;
            case "SqlDateTime": return ((SqlDateTime)o).Value;
            case "SqlDecimal": return ((SqlDecimal)o).Value;
            case "SqlDouble": return ((SqlDouble)o).Value;
            case "SqlGuid": return ((SqlGuid)o).Value;
            case "SqlInt16": return ((SqlInt16)o).Value;
            case "SqlInt32": return ((SqlInt32)o).Value;
            case "SqlInt64": return ((SqlInt64)o).Value;
            case "SqlMoney": return ((SqlMoney)o).Value;
            case "SqlSingle": return ((SqlSingle)o).Value;
            case "SqlString": return ((SqlString)o).Value;
            case "SqlXml": return ((SqlXml)o).Value;
            case "DBNull": return null;
            case "DateTime": return (DateTime)o;
            default: throw new Exception("Type not recognised");
        }
    }

    internal static object read(System.IO.BinaryReader r)
    {
        string s = r.ReadString();
        switch (s)
        {
            case "null": return null;
            case "Boolean": return r.ReadBoolean();
            case "Byte": return r.ReadByte();
            case "DateTime": return new DateTime(r.ReadInt64());
            case "Decimal": return r.ReadDecimal();
            case "Double": return r.ReadDouble();
            case "Guid": return new Guid(r.ReadBytes(16));
            case "Int16": return r.ReadInt16();
            case "Int32": return r.ReadInt32();
            case "Int64": return r.ReadInt64();
            case "Single": return r.ReadSingle();
            case "String": return r.ReadString();
            default: return null;
        }
    }

    internal static void write(System.IO.BinaryWriter w, object o)
    {
        if (o == null)
        {
            w.Write("null");
            return;
        }
        string s = o.GetType().Name;
        w.Write(s);
        switch (s)
        {
            case "Boolean": w.Write((bool)o); break;
            case "Byte": w.Write((byte)o); break;
            case "DateTime": w.Write(((DateTime)o).Ticks); break;
            case "Decimal": w.Write((decimal)o); break;
            case "Double": w.Write((double)o); break;
            case "Guid": w.Write(((Guid)o).ToByteArray()); break;
            case "Int16": w.Write((short)o); break;
            case "Int32": w.Write((int)o); break;
            case "Int64": w.Write((long)o); break;
            case "Single": w.Write((float)o); break;
            case "String": w.Write((string)o); break;
        }
    }


    #region Numberline / Timeline

    public class numberlist : IEnumerable
    {
        private double start;
        private double end;
        private double interval;

        public numberlist(double s, double e, double i)
        {
            start = s;
            end = e;
            interval = i;

            if (start != end && interval == 0)
                throw new Exception("ArithmeticSeries: interval must be non-zero");

            if ((start > end && interval > 0) || (start < end && interval < 0))
                throw new Exception("ArithmeticSeries: interval must have the same sign as [end - start]");
        }

        public IEnumerator GetEnumerator()
        {
            return new numberlistenumerator(start, end, interval);
        }
    }

    public class numberlistenumerator : IEnumerator
    {
        private double start;
        private double end;
        private double interval;
        private double value;
        private bool active;
        private int count;

        public numberlistenumerator(double s, double e, double i)
        {
            start = s;
            end = e;
            interval = i;
            value = start;
            active = false;
            count = 1;
        }

        public bool MoveNext()
        {
            if (!active)
            {
                value = start;
                active = true;
            }
            else value = start + interval * count++;
            if (start < end) return (value <= end);
            return (value >= end);
        }

        public void Reset()
        {
            active = false;
        }

        public object Current
        {
            get
            {
                if ((start < end && value <= end || start > end && value >= end) && active)
                    return value;
                else
                    throw new InvalidOperationException();
            }
        }
    }

    [SqlFunction(FillRowMethodName = "ParseDataRow", TableDefinition = "value FLOAT", SystemDataAccess = SystemDataAccessKind.Read)]
    public static System.Collections.IEnumerable NUMBERLINE(double start, double end, double interval)
    {
        ValidateKey();
        numberlist nl = new numberlist(start, end, interval);
        return nl;
    }

    [SqlFunction(FillRowMethodName = "ParseDateTimeRow", TableDefinition = "value DATETIME", SystemDataAccess = SystemDataAccessKind.Read)]
    public static System.Collections.IEnumerable TIMELINE(DateTime start, DateTime end, double interval, string datepart)
    {
        ValidateKey();
        List<DateTime> theList = new List<DateTime>();

        switch (datepart)
        {
            case ("year"):
            case ("yyyy"):
            case ("yy"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddYears((int)interval)) theList.Add(d);
                    break;
                }
            case ("quarter"):
            case ("qq"):
            case ("q"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddMonths(3 * (int)interval)) theList.Add(d);
                    break;
                }
            case ("month"):
            case ("mm"):
            case ("m"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddMonths((int)interval)) theList.Add(d);
                    break;
                }
            case ("dayofyear"):
            case ("dy"):
            case ("y"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddDays(interval)) theList.Add(d);
                    break;
                }
            case ("day"):
            case ("dd"):
            case ("d"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddDays(interval)) theList.Add(d);
                    break;
                }
            case ("week"):
            case ("wk"):
            case ("ww"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddDays(7 * interval)) theList.Add(d);
                    break;
                }
            case ("hour"):
            case ("hh"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddHours(interval)) theList.Add(d);
                    break;
                }
            case ("minute"):
            case ("mi"):
            case ("n"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddMinutes(interval)) theList.Add(d);
                    break;
                }
            case ("second"):
            case ("ss"):
            case ("s"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddSeconds(interval)) theList.Add(d);
                    break;
                }
            case ("millisecond"):
            case ("ms"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddMilliseconds(interval)) theList.Add(d);
                    break;
                }
            case ("microsecond"):
            case ("mcs"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddMilliseconds(interval / 1000.0)) theList.Add(d);
                    break;
                }
            case ("nanosecond"):
            case ("ns"):
                {
                    for (DateTime d = start; end > start ? d <= end : d >= end; d = d.AddMilliseconds(interval / 1e-6)) theList.Add(d);
                    break;
                }
            default:
                {
                    throw new System.Exception("Invalid datepart parameter specified");
                }
        }
        return theList;
    }

    //--- Define the call-back function that will parse each row in the enumeration ---
    public static void ParseDataRow(object item, out double value)
    {
        value = (double)item;
    }

    //--- Define the call-back function that will parse each row in the enumeration ---
    public static void ParseDateTimeRow(object item, out DateTime value)
    {
        value = (DateTime)item;
    }

    #endregion

    #region Randoms

    static readonly Random rand = new Random();

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime RANDDATEBETWEEN(SqlDateTime a, SqlDateTime b)
    {
        ValidateKey();
        if (a.IsNull) a = new SqlDateTime(DateTime.MinValue);
        if (b.IsNull) b = new SqlDateTime(DateTime.MaxValue);
        int ax = a.DayTicks;
        int bx = b.DayTicks;
        int c = rand.Next(ax, bx);
        return new SqlDateTime(c, 0);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime RANDTIMEBETWEEN(SqlDateTime a, SqlDateTime b)
    {
        ValidateKey();
        int c, ay, by;
        if (a.IsNull) a = new SqlDateTime(DateTime.MinValue);
        if (b.IsNull) b = new SqlDateTime(a.Value.Date.AddDays(1).AddMilliseconds(-1));
        if (a.DayTicks != b.DayTicks)
        {
            int ax = a.DayTicks;
            int bx = b.DayTicks;
            c = rand.Next(ax, bx);
        }
        else c = a.DayTicks;
        if (c == a.DayTicks) ay = a.TimeTicks;
        else ay = 0;
        if (c == b.DayTicks) by = b.TimeTicks;
        else by = SqlDateTime.SQLTicksPerHour * 24;
        int d = rand.Next(ay, by);
        return new SqlDateTime(c, d);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDouble RANDOM()
    {
        ValidateKey();
        return new SqlDouble(rand.NextDouble());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 RANDBETWEEN(SqlInt32 a, SqlInt32 b)
    {
        ValidateKey();
        return new SqlInt32(rand.Next(a.Value, b.Value));
    }


    #endregion

    #region Date functions

    //[SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    //public static SqlBoolean ISEOFEB(SqlDateTime d)
    //{
    //    if (d.IsNull) return SqlBoolean.Null;
    //    return d.Value.Month == 2 && ISEOMONTH(d);
    //}

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean ISEOMONTH(SqlDateTime d)
    {
        ValidateKey();
        if (d.IsNull) return SqlBoolean.Null;
        return (d.Value.AddDays(1)).Day == 1;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean ISLEAPYEAR(SqlInt32 y)
    {
        ValidateKey();
        if (y.IsNull) return SqlBoolean.Null;
        return DateTime.IsLeapYear(y.Value);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime EOMONTH(SqlDateTime d, SqlInt32 months)
    {
        ValidateKey();
        if (d.IsNull) return SqlDateTime.Null;
        DateTime d2 = d.Value;
        if (!months.IsNull) d2 = d2.AddMonths(months.Value);
        d2 = d2.AddDays(1 - d2.Day);
        d2 = d2.AddMonths(1);
        d2 = d2.AddDays(-1);
        return d2;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime SOMONTH(SqlDateTime d, SqlInt32 months)
    {
        ValidateKey();
        if (d.IsNull) return SqlDateTime.Null;
        DateTime d2 = d.Value;
        if (!months.IsNull) d2 = d2.AddMonths(months.Value);
        d2 = d2.AddDays(-d2.Day + 1);
        return d2;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 DAYSINMONTH(SqlDateTime d)
    {
        ValidateKey();
        if (d.IsNull) return SqlInt32.Null;
        return EOMONTH(d, 0).Value.Day;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime NTHINMONTH(SqlDateTime now, SqlInt32 dow, SqlInt32 nth)
    {
        ValidateKey();
        if (now.IsNull || dow.IsNull || nth.IsNull) return SqlDateTime.Null;
        if (nth < 1 || nth > 5) throw new Exception("Instance Qualifier Out of Range: must be between 1 and 5");
        int dw = dow.Value % 7;
        DateTime dt = (DateTime)now;
        IFormatProvider culture = new CultureInfo("en-GB", true);
        string dts = dt.ToString("YYYY-MM-01");
        DateTime nim = DateTime.ParseExact(dts, "YYYY-MM-dd", culture);
        int increment = (((int)dow - (int)nim.DayOfWeek + 7) % 7) + 7 * ((int)nth - 1);
        if (increment >= DateTime.DaysInMonth(nim.Year, nim.Month)) return SqlDateTime.Null;
        return (SqlDateTime)nim.AddDays((double)increment);
    }

    private static SqlInt32 GetInt(object o)
    {
        switch (o.GetType().Name)
        {
            case "SqlByte": return ((SqlByte)o).ToSqlInt32();
            case "SqlDecimal": return ((SqlDecimal)o).ToSqlInt32();
            case "SqlDouble": return ((SqlDouble)o).ToSqlInt32();
            case "SqlInt16": return ((SqlInt16)o).ToSqlInt32();
            case "SqlInt32": return (SqlInt32)o;
            case "SqlInt64": return ((SqlInt64)o).ToSqlInt32();
            case "SqlMoney": return ((SqlMoney)o).ToSqlInt32();
            case "SqlSingle": return ((SqlSingle)o).ToSqlInt32();
            default: throw new Exception(string.Format("Numeric SQL type expected - got {0:G}", o.GetType().Name));
        }
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 NETWORKDAYS(SqlDateTime start, SqlDateTime end, object weekend, SqlChars holidays)
    {
        ValidateKey();
        if (start.IsNull || end.IsNull) return SqlInt32.Null;
        int sign = start <= end ? 1 : -1;
        if (start > end)
        {
            SqlDateTime tmp = start;
            start = end;
            end = tmp;
        }
        int stage1 = end.Value.Subtract(start.Value).Days + 1;
        int d1 = (int)start.Value.DayOfWeek;
        int d2 = (int)end.Value.DayOfWeek;
        int weekendcount;
        bool[] isweekend = GetWeekends(weekend, out weekendcount, "NETWORKDAYS");
        int remnant = (stage1 - 1) % 7;
        int stage2 = stage1 - weekendcount * (stage1 - remnant) / 7;
        for (int i = d1; i <= d1 + remnant; i++) if (isweekend[i % 7]) stage2--;
        if (holidays.IsNull) return stage2 * sign;
        List<DateTime> hols = GetHolidays(holidays, "NETWORKDAYS");
        foreach(DateTime hday in hols)
            if (!isweekend[(int)hday.DayOfWeek] && hday >= start.Value && hday <= end.Value) stage2--;
        return stage2 * sign;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean ISWORKDAY(SqlDateTime d, object weekend, SqlChars holidays)
    {
        ValidateKey();
        if (d.IsNull) return SqlBoolean.Null;
        int weekendcount;
        bool[] isweekend = GetWeekends(weekend, out weekendcount, "ISWORKDAY");
        int d1 = (int)d.Value.DayOfWeek;
        if (isweekend[d1]) return false;
        if (holidays.IsNull) return true;
        List<DateTime> hols = GetHolidays(holidays, "ISWORKDAY");
        foreach (DateTime hday in hols) if (hday == d.Value) return false;
        return true;
    }

    private static bool[] GetWeekends(object weekend, out int weekendcount, string caller)
    {
        weekendcount = 0;
        bool[] isweekend;
        if (weekend == DBNull.Value) weekend = new SqlInt32(1);
        if (weekend.GetType().Name == "SqlString")
        {
            isweekend = new bool[7];
            string w = ((SqlString)weekend).Value;
            if (w.Length != 7)
                throw new ArgumentException(string.Format("{0:G}: String value for weekend must be 7 characters long (got '{1:G}')", caller, w));
            for (int i = 0; i < 7; i++)
            {
                if (w[i] == '0') isweekend[i] = false;
                else if (w[i] == '1')
                {
                    isweekend[i] = true;
                    weekendcount++;
                }
                else throw new ArgumentException(string.Format("{0:G}: Only 1s and 0s are permitted in a string value for weekend (got '{1:G}')", caller, w));
            }
        }
        else
        {
            weekendcount = 1;
            int wi = GetInt(weekend).Value;
            if(wi < 1 || (wi > 7 && wi < 11) || wi > 17)
                throw new ArgumentException(string.Format("{0:G}: Integer value for weekend must be in the range 1-7 or 11-17 (got {1:G})", caller, wi));
            int x1, x2;
            if (wi < 8)
            {
                x1 = (wi + 5) % 7;
                x2 = (wi + 6) % 7;
                weekendcount = 2;
            }
            else x1 = x2 = wi % 11;
            isweekend = new bool[7];
            for (int i = 0; i < 7; i++)
            {
                isweekend[i] = i == x1 || i == x2 ? true : false;
            }
        }
        return isweekend;
    }

    private static List<DateTime> GetHolidays(SqlChars holidays, string caller)
    {
        string[] hols = new string(holidays.Value).Split(',');
        List<DateTime> hlist = new List<DateTime>();
        DateTime hday;
        for (int i = 0; i < hols.Length; i++)
        {
            if (hols[i] == "") continue;
            if (!DateTime.TryParse(hols[i], out hday))
                throw new ArgumentException(string.Format("{0:G}: Could not parse '{1:G}' as a valid holiday date", caller, hols[i]));
            if (!hlist.Exists(p => p.Date == hday.Date)) hlist.Add(hday);
        }
        return hlist;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime WORKDAY(SqlDateTime start, SqlInt32 days, object weekend, SqlChars holidays)
    {
        ValidateKey();
        if (start.IsNull || days.IsNull) return SqlDateTime.Null;
        int sign = days.Value >= 0 ? 1 : -1;
        int weekendcount;
        bool[] isweekend = GetWeekends(weekend, out weekendcount,  "WORKDAY");
        if (days.Value == 0) return start;
        if (weekendcount >= 7) return SqlDateTime.Null;
        List<DateTime> hols = GetHolidays(holidays, "WORKDAY");
        DateTime d2 = start.Value;
        for (int i = 0; i < Math.Abs(days.Value); i++)
        {
            d2 = d2.AddDays(sign);
            while(isweekend[(int)d2.DayOfWeek % 7] || hols.Exists(p => p.Date == d2.Date)) d2 = d2.AddDays(sign);
        }
        return d2;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 WORKDAYSINMONTH(SqlDateTime d, object weekend, SqlChars holidays)
    {
        ValidateKey();
        if (d.IsNull) return SqlInt32.Null;
        return NETWORKDAYS(SOMONTH(d, 0), EOMONTH(d, 0), weekend, holidays);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime DATEONLY(SqlDateTime d)
    {
        ValidateKey();
        if (d.IsNull) return SqlDateTime.Null;
        return (d.Value.Date);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime TIMEONLY(SqlDateTime d)
    {
        ValidateKey();
        if (d.IsNull) return SqlDateTime.Null;
        DateTime dd = d.Value;
        return new SqlDateTime(1900, 01, 01, dd.Hour, dd.Minute, dd.Second, dd.Millisecond);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime DATE(SqlInt32 year, SqlInt32 month, SqlInt32 day)
    {
        ValidateKey();
        int y, m, d;
        y = year.IsNull ? DateTime.Today.Year : year.Value;
        m = month.IsNull ? DateTime.Today.Month : month.Value;
        d = day.IsNull ? DateTime.Today.Day : day.Value;
        DateTime dd = new DateTime(y, m, d);
        return dd;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime DATETIME(SqlInt32 year, SqlInt32 month, SqlInt32 day, SqlInt32 hour, SqlInt32 minute, SqlInt32 second, SqlInt32 millisecond)
    {
        ValidateKey();
        int y, m, d, h, n, s, ms;

        y = year.IsNull ? DateTime.Today.Year : year.Value;
        m = month.IsNull ? DateTime.Today.Month : month.Value;
        d = day.IsNull ? DateTime.Today.Day : day.Value;
        h = hour.IsNull ? DateTime.Now.Hour : hour.Value;
        n = minute.IsNull ? DateTime.Now.Minute : minute.Value;
        s = second.IsNull ? DateTime.Now.Second : second.Value;
        ms = millisecond.IsNull ? DateTime.Now.Millisecond : millisecond.Value;
        DateTime dd = new DateTime(y, m, d, h, n, s, ms);
        return dd;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime TIME(SqlInt32 hour, SqlInt32 minute, SqlInt32 second, SqlInt32 millisecond)
    {
        ValidateKey();
        int h, n, s, ms;

        h = hour.IsNull ? DateTime.Now.Hour : hour.Value;
        n = minute.IsNull ? DateTime.Now.Minute : minute.Value;
        s = second.IsNull ? DateTime.Now.Second : second.Value;
        ms = millisecond.IsNull ? DateTime.Now.Millisecond : millisecond.Value;
        DateTime dd = new DateTime(1900, 01, 01, h, n, s, ms);
        return dd;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime FROMXLDATE(SqlDouble xld)
    {
        ValidateKey();
        if (xld.IsNull) return SqlDateTime.Null;
        if (xld.Value < 61) throw new Exception("FROMXLDATE: Values below 61 (March 1, 1900) are not supported because of a bug in Excel");
        return DateTime.FromOADate(xld.Value);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDouble TOXLDATE(SqlDateTime xld)
    {
        ValidateKey();
        if (xld.IsNull) return SqlDouble.Null;
        if (xld.Value < new DateTime(1900, 3, 1)) throw new Exception("TOXLDATE: Dates before March 1, 1900 are not supported because of a bug in Excel");
        return xld.Value.ToOADate();
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlDateTime ADDDAYS(SqlDateTime xld, SqlDouble inc)
    {
        ValidateKey();
        if (xld.IsNull || inc.IsNull) return SqlDateTime.Null;
        return xld.Value.Add(TimeSpan.FromDays(inc.Value));
    }  
    
    #endregion

    #region String functions

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRTOKEN(SqlChars input, SqlChars delimiters, SqlInt32 n)
    {
        ValidateKey();
        if (input.IsNull || n.IsNull) return SqlChars.Null;
        if (n.Value < 1) throw new Exception("STRTOKEN: parameter n must be >= 1");
        char[] dl = delimiters.IsNull || delimiters.Length == 0 ? new char[] { ',' } : delimiters.Value;
        string Text = new string(input.Value);
        string[] Words = Text.Split(dl);
        if (Words.Length <= (int)n-1) return SqlChars.Null;
        return new SqlChars(Words[(int)n-1]);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 STRTOKENCOUNT(SqlChars input, SqlChars delimiters)
    {
        ValidateKey();
        if (input.IsNull) return SqlInt32.Null;
        char[] dl = delimiters.IsNull || delimiters.Length == 0 ? new char[] { ',' } : delimiters.Value;
        string Text = new string(input.Value);
        string[] Words = Text.Split(dl);
        return (SqlInt32)Words.Length;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean STRTOKENMATCHESANY(SqlChars list1, SqlChars list2, SqlChars delimiters, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (list1.IsNull || list2.IsNull) return SqlBoolean.Null;
        char[] dl = delimiters.IsNull || delimiters.Length == 0 ? new char[] { ',' } : delimiters.Value;
        string t1 = new string(list1.Value);
        string t2 = new string(list2.Value);
        List<string>Words1 = new List<string>(t1.Split(dl));
        List<string>Words2 = new List<string>(t2.Split(dl));
        if (casesensitive.IsTrue)
        {
            foreach (string s in Words1) if (Words2.Exists(p => p.Equals(s, StringComparison.InvariantCulture))) return true;
        }
        else
        {
            foreach (string s in Words1) if (Words2.Exists(p => p.Equals(s, StringComparison.InvariantCultureIgnoreCase))) return true;
        }
        return false;
    }


    [SqlFunction(FillRowMethodName = "ParseStringRow", TableDefinition = "value NVARCHAR(MAX)", DataAccess = DataAccessKind.Read)]
    public static System.Collections.IEnumerable STRTOKENS(SqlChars input, SqlChars delimiters)
    {
        ValidateKey();
        string item;
        List<string> theList = new List<string>();

        int j = STRTOKENCOUNT(input, delimiters).Value;
        for (int i = 0; i < j; i++)
        {
            item = new string(STRTOKEN(input, delimiters, i + 1).Value);
            theList.Add(item);
        }
        return theList;
    }

    //--- Define the call-back function that will parse each row in the enumeration ---
    public static void ParseStringRow(object item, out string value)
    {
        value = (string)item;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRCLEAN(SqlChars input)
    {
        ValidateKey();
        if (input.IsNull) return SqlChars.Null;
        char[] text = input.Value;
        StringBuilder sb = new StringBuilder(text.Length);
        for (int i = 0; i < text.Length; i++) if(!char.IsControl(text[i])) sb.Append(text[i]);
        return new SqlChars(sb.ToString());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlString CRLF()
    {
        ValidateKey();
        StringBuilder sb = new StringBuilder(2);
        sb.Append("\r\n");
        return new SqlString(sb.ToString());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRCAT2(SqlChars a, SqlChars b, SqlChars separator)
    {
        ValidateKey();
        if (a.IsNull) return b;
        if (b.IsNull) return a;
        StringBuilder sb = new StringBuilder();
        sb.Append(a.Value);
        if(!separator.IsNull) sb.Append(separator.Value);
        sb.Append(b.Value);
        return new SqlChars(sb.ToString());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRINSERT(SqlChars input, SqlChars insert, SqlInt32 index)
    {
        ValidateKey();
        if (insert.IsNull || index.IsNull) return input;
        if (input.IsNull) return SqlChars.Null;
        if (index.Value < 1 || index.Value > input.Value.Length + 1)
            throw new ArgumentException(string.Format("STRINSERT: Insert position expected between 1 and {0:G} (got {1:G})", input.Value.Length + 1, index.Value));
        string text = new string(input.Value);
        string intext = new string(insert.Value);
        text = text.Insert(index.Value - 1, intext);
        return new SqlChars(text);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRREMOVE(SqlChars input, SqlInt32 index, SqlInt32 count)
    {
        ValidateKey();
        if (index.IsNull) return input;
        if (input.IsNull) return SqlChars.Null;
        if (index.Value < 1 || index.Value > input.Value.Length)
            throw new ArgumentException(string.Format("STRREMOVE: Start position for removal expected between 1 and {0:G} (got {1:G})", input.Value.Length, index.Value));
        string text = new string(input.Value);
        int c;
        if (count.IsNull || index.Value + count.Value > input.Value.Length) c = input.Value.Length - index.Value + 1;
        else c = count.Value;
        text = text.Remove(index.Value - 1, c);
        return new SqlChars(text);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean STREQUALS(SqlChars a, SqlChars b, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlBoolean.Null;
        string sa = new string(a.Value);
        string sb = new string(b.Value);
        StringComparison sc = casesensitive.IsNull || casesensitive.Value ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
        return new SqlBoolean(sa.Equals(sb, sc));
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 STRCOMPARE(SqlChars a, SqlChars b, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlInt32.Null;
        string sa = new string(a.Value);
        string sb = new string(b.Value);
        StringComparison sc = casesensitive.IsNull || casesensitive.Value ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
        return new SqlInt32(string.Compare(sa, sb, sc));
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean STRCONTAINS(SqlChars a, SqlChars b, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlBoolean.Null;
        string sa = new string(a.Value);
        string sb = new string(b.Value);
        StringComparison sc = casesensitive.IsNull || casesensitive.Value ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
        return new SqlBoolean(sa.IndexOf(sb, sc) >= 0);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 STRCONTAINSCOUNT(SqlChars a, SqlChars b, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlInt32.Null;
        string sa = new string(a.Value);
        string sb = new string(b.Value);
        StringComparison sc = casesensitive.IsNull || casesensitive.Value ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
        int n = -1, i = 0;
        while (true)
        {
            n++;
            i = sa.IndexOf(sb, i, sc) + 1;
            if (i == 0) break;
        }
        return n;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean STRSTARTSWITH(SqlChars a, SqlChars b, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlBoolean.Null;
        string sa = new string(a.Value);
        string sb = new string(b.Value);
        StringComparison sc = casesensitive.IsNull || casesensitive.Value ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
        return new SqlBoolean(sa.StartsWith(sb, sc));
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 STRINDEX(SqlChars a, SqlChars b, SqlInt32 n, SqlInt32 c, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlInt32.Null;
        string sa = new string(a.Value);
        string sb = new string(b.Value);
        int nn = n.IsNull ? 0 : n.Value - 1;
        if (nn < 0 || nn >= sa.Length)
            throw new ArgumentException(string.Format("STRINDEX: Start position expected to be between 1 and {0:G} (got {1:G})", sa.Length, nn + 1));
        int cc = c.IsNull || c.Value + nn > sa.Length ? sa.Length - nn : c.Value;
        StringComparison sc = casesensitive.IsNull || casesensitive.Value ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
        return new SqlInt32(sa.IndexOf(sb, nn, cc, sc) + 1);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 STRINDEXOFANY(SqlChars a, SqlChars b, SqlInt32 n, SqlInt32 c)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlInt32.Null;
        string sa = new string(a.Value);
        int nn = n.IsNull ? 0 : n.Value - 1;
        if (nn < 0 || nn >= sa.Length)
            throw new ArgumentException(string.Format("STRINDEXOFANY: Start position expected to be between 1 and {0:G} (got {1:G})", sa.Length, nn + 1));
        int cc = c.IsNull || c.Value + nn > sa.Length ? sa.Length - nn : c.Value;
        return new SqlInt32(sa.IndexOfAny(b.Value, nn, cc) + 1);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 STRLASTINDEX(SqlChars a, SqlChars b, SqlInt32 n, SqlInt32 c, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlInt32.Null;
        string sa = new string(a.Value);
        string sb = new string(b.Value);
        int nn = n.IsNull ? sa.Length - 1 : n.Value - 1;
        if (nn < 0 || nn >= sa.Length)
            throw new ArgumentException(string.Format("STRLASTINDEX: Start position expected to be between 1 and {0:G} (got {1:G})", sa.Length, nn + 1));
        int cc = c.IsNull || c.Value > nn + 1 ? nn + 1 : c.Value;
        StringComparison sc = casesensitive.IsNull || casesensitive.Value ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
        return new SqlInt32(sa.LastIndexOf(sb, nn, cc, sc) + 1);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 STRLASTINDEXOFANY(SqlChars a, SqlChars b, SqlInt32 n, SqlInt32 c)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlInt32.Null;
        string sa = new string(a.Value);
        int nn = n.IsNull ? sa.Length - 1 : n.Value - 1;
        if (nn < 0 || nn >= sa.Length)
            throw new ArgumentException(string.Format("STRLASTINDEXOFANY: Start position expected to be between 1 and {0:G} (got {1:G})", sa.Length, nn + 1));
        int cc = c.IsNull || c.Value > nn + 1 ? nn + 1 : c.Value;
        return new SqlInt32(sa.LastIndexOfAny(b.Value, nn, cc) + 1);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean STRENDSWITH(SqlChars a, SqlChars b, SqlBoolean casesensitive)
    {
        ValidateKey();
        if (a.IsNull || b.IsNull) return SqlBoolean.Null;
        string sa = new string(a.Value);
        string sb = new string(b.Value);
        StringComparison sc = casesensitive.IsNull || casesensitive.Value ? StringComparison.InvariantCulture : StringComparison.InvariantCultureIgnoreCase;
        return new SqlBoolean(sa.EndsWith(sb, sc));
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRIPALL(SqlChars input, SqlChars stripchars)
    {
        ValidateKey();
        if (input.IsNull) return SqlChars.Null;
        char[] text = input.Value;
        StringBuilder sb = new StringBuilder(text.Length);
        if (stripchars.IsNull || stripchars.Value.Length == 0)
        {
            for (int i = 0; i < text.Length; i++) if (!char.IsPunctuation(text[i]) && !char.IsSymbol(text[i])) sb.Append(text[i]);
        }
        else
        {
            List<char> striplist = new List<char>(stripchars.Value);
            for (int i = 0; i < text.Length; i++) if (!striplist.Contains(text[i])) sb.Append(text[i]);
        }
        return new SqlChars(sb.ToString());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRPROPER(SqlChars input)
    {
        ValidateKey();
        if (input.IsNull) return SqlChars.Null;
        bool upper = true;
        char[] text = input.Value;
        StringBuilder sb = new StringBuilder(text.Length);
        for (int i = 0; i < text.Length; i++)
        {
            if (char.IsWhiteSpace(text[i]) || char.IsPunctuation(text[i])) upper = true;
            if (char.IsLetter(text[i]))
            {
                if (upper)
                {
                    sb.Append(char.ToUpper(text[i]));
                    upper = false;
                }
                else sb.Append(char.ToLower(text[i]));
            }
            else sb.Append(text[i]);
        }
        return new SqlChars(sb.ToString());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRIP(SqlChars input, SqlChars stripchars)
    {
        return STRIPLEFT(STRIPRIGHT(input, stripchars), stripchars);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRIPLEFT(SqlChars input, SqlChars stripchars)
    {
        ValidateKey();
        if (input.IsNull) return SqlChars.Null;
        char[] text = input.Value;
        StringBuilder sb = new StringBuilder(text.Length);
        List<char> striplist = stripchars.IsNull || stripchars.Value.Length == 0 ? null : new List<char>(stripchars.Value);
        int i = 0;
        for (; i < text.Length; i++)
        {
            if ( striplist == null && !char.IsPunctuation(text[i]) && !char.IsSymbol(text[i]) 
                 || striplist != null && !striplist.Contains(text[i]) )
            {
                sb.Append(text[i++]);
                break;
            }
        }
        for (; i < text.Length; i++) sb.Append(text[i]);
        return new SqlChars(sb.ToString());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRIPRIGHT(SqlChars input, SqlChars stripchars)
    {
        ValidateKey();
        if (input.IsNull) return SqlChars.Null;
        char[] text = input.Value;
        StringBuilder sb = new StringBuilder(text.Length);
        List<char> striplist = stripchars.IsNull || stripchars.Value.Length == 0 ? null : new List<char>(stripchars.Value);
        int i = text.Length - 1;
        for (; i >= 0; i--)
        {
            if (striplist == null && !char.IsPunctuation(text[i]) && !char.IsSymbol(text[i])
                 || striplist != null && !striplist.Contains(text[i]))
            {
                sb.Insert(0, text[i--]);
                break;
            }
        }
        for (; i >= 0; i--) sb.Insert(0, text[i]);
        return new SqlChars(sb.ToString());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRPADLEFT(SqlChars input, SqlInt32 width, SqlString padchar)
    {
        ValidateKey();
        if (input.IsNull || width.IsNull) return SqlChars.Null;
        string text = new string(input.Value);
        char p = padchar.IsNull || padchar.Value.Length == 0 ? ' ' : padchar.Value[0];
        text = text.PadLeft(width.Value, p);
        return new SqlChars(text);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRPADRIGHT(SqlChars input, SqlInt32 width, SqlString padchar)
    {
        ValidateKey();
        if (input.IsNull || width.IsNull) return SqlChars.Null;
        string text = new string(input.Value);
        char p = padchar.IsNull || padchar.Value.Length == 0 ? ' ' : padchar.Value[0];
        text = text.PadRight(width.Value, p);
        return new SqlChars(text);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars STRPADMID(SqlChars input, SqlInt32 width, SqlString padchar)
    {
        ValidateKey();
        if (input.IsNull || width.IsNull) return SqlChars.Null;
        string text = new string(input.Value);
        char p = padchar.IsNull || padchar.Value.Length == 0 ? ' ' : padchar.Value[0];
        int r = (width.Value - text.Length) / 2 + text.Length;
        text = text.PadLeft(r, p);
        text = text.PadRight(width.Value, p);
        return new SqlChars(text);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlString FORMAT(object input, SqlString format)
    {
        ValidateKey();
        if (input == DBNull.Value) return SqlString.Null;
        string result;

        switch (input.GetType().Name)
        {
            case "SqlBinary":
                SqlBinary bin = (SqlBinary)input;
                if(bin.IsNull) result = null;
                else result = BitConverter.ToString(bin.Value);
                break;
            case "SqlBoolean": 
                SqlBoolean bol = (SqlBoolean)input;
                if (bol.IsNull) result = null;
                else result = bol.Value.ToString();
                break;
            case "SqlByte":
                SqlByte byt = (SqlByte)input;
                if (byt.IsNull) result = null;
                else result = byt.Value.ToString(format.Value);
                break;
            case "SqlBytes":
                SqlBytes byts = (SqlBytes)input;
                if (byts.IsNull) result = null;
                else result = BitConverter.ToString(byts.Value);
                break;
            case "SqlChars":
                SqlChars chrs = (SqlChars)input;
                if (chrs.IsNull) result = null;
                else result = new string(chrs.Value);
                break;
            case "SqlDateTime":
                SqlDateTime dt = (SqlDateTime)input;
                if (dt.IsNull) result = null;
                else result = dt.Value.ToString(format.Value);
                break;
            case "SqlDecimal":
                SqlDecimal dec = (SqlDecimal)input;
                if (dec.IsNull) result = null;
                else result = dec.Value.ToString(format.Value);
                break;
            case "SqlDouble":
                SqlDouble dbl = (SqlDouble)input;
                if (dbl.IsNull) result = null;
                else result = dbl.Value.ToString(format.Value);
                break;
            case "SqlGuid":
                SqlGuid guid = (SqlGuid)input;
                if (guid.IsNull) result = null;
                else result = guid.Value.ToString(format.Value);
                break;
            case "SqlInt16":
                SqlInt16 i16 = (SqlInt16)input;
                if (i16.IsNull) result = null;
                else result = i16.Value.ToString(format.Value);
                break;
            case "SqlInt32":
                SqlInt32 i32 = (SqlInt32)input;
                if (i32.IsNull) result = null;
                else result = i32.Value.ToString(format.Value);
                break;
            case "SqlInt64":
                SqlInt64 i64 = (SqlInt64)input;
                if (i64.IsNull) result = null;
                else result = i64.Value.ToString(format.Value);
                break;
            case "SqlMoney":
                SqlMoney mon = (SqlMoney)input;
                if (mon.IsNull) result = null;
                else result = mon.Value.ToString(format.Value);
                break;
            case "SqlSingle":
                SqlSingle sgl = (SqlSingle)input;
                if (sgl.IsNull) result = null;
                else result = sgl.Value.ToString(format.Value);
                break;
            case "SqlString":
                SqlString strg = (SqlString)input;
                if (strg.IsNull) result = null;
                else result = strg.Value;
                break;
            case "SqlXml":
                SqlXml xml = (SqlXml)input;
                if (xml.IsNull) result = null;
                else result = xml.Value;
                break;
            case "DBNull": 
                result = null;
                break;
            default: 
                result = input.ToString();
                break;
        }
        if (result == null) return SqlString.Null;
        return new SqlString(result);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars TRIM(SqlChars input)
    {
        ValidateKey();
        if (input.IsNull) return SqlChars.Null;
        string text = new string(input.Value);
        return new SqlChars(text.Trim());
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static object TRYPARSE(SqlChars input, SqlChars type, SqlChars format)
    {
        ValidateKey();
        long l;
        bool b;
        int i;
        decimal d;
        short u;
        byte y;
        double dd;
        float sg;
        DateTime dt;
        Guid g;

        if (input.IsNull || type.IsNull) return DBNull.Value;
        string text = new string(input.Value);
        string sqltype = new string(type.Value).ToLower();
        string fmt = format.IsNull ? null : new string(format.Value);
        switch (sqltype)
        {
            case "bigint":
                if (!long.TryParse(text, out l)) return DBNull.Value;
                return new SqlInt64(l);
            case "int":
                if (!int.TryParse(text, out i)) return DBNull.Value;
                return new SqlInt32(i);
            case "smallint":
                if (!short.TryParse(text, out u)) return DBNull.Value;
                return new SqlInt16(u);
            case "tinyint":
                if (!byte.TryParse(text, out y)) return DBNull.Value;
                return new SqlByte(y);
            case "bit":
                if (bool.TryParse(text, out b)) return new SqlBoolean(b);
                if (int.TryParse(text, out i)) return new SqlBoolean(i);
                return DBNull.Value;
            case "decimal":
            case "numeric":
                if (!decimal.TryParse(text, out d)) return DBNull.Value;
                return new SqlDecimal(d);
            case "money":
            case "smallmoney":
                if (!decimal.TryParse(text, out d)) return DBNull.Value;
                return new SqlMoney(d);
            case "float":
                if (!double.TryParse(text, out dd)) return DBNull.Value;
                return new SqlDouble(dd);
            case "real":
                if (!float.TryParse(text, out sg)) return DBNull.Value;
                return new SqlSingle(sg);
            case "date":
            case "datetime":
            case "datetime2":
            case "datetimeoffset":
            case "smalldatetime":
            case "time":
                if (fmt == null)
                {
                    if (!DateTime.TryParse(text, out dt)) return DBNull.Value;
                    return new SqlDateTime(dt);
                }
                if (!DateTime.TryParseExact(text, fmt, DateTimeFormatInfo.CurrentInfo, DateTimeStyles.None, out dt)) return DBNull.Value;
                return new SqlDateTime(dt);                
            case "char":
            case "nchar":
                return new SqlChars(text.Substring(0, 1));
            case "varchar":
            case "text":
            case "nvarchar":
            case "ntext":
                return input;
            case "binary":
            case "varbinary":
            case "image":
                List<byte> bl = new List<byte>();
                int j = 0;
                if(text.StartsWith("0x")) j = 2;
                for (; j < text.Length; j += 2)
                {
                    if (!byte.TryParse(text.Substring(j, 2), NumberStyles.HexNumber, CultureInfo.CurrentCulture, out y)) return DBNull.Value;
                    bl.Add(y);
                }
                return new SqlBinary(bl.ToArray());
            case "uniqueidentifier":
                g = new Guid(text);
                return new SqlGuid(g);
            default:
                return DBNull.Value;
        }
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars RXREPLACE(SqlChars input, SqlChars pattern, SqlChars replacement, SqlInt32 start)
    {
        ValidateKey();
        if (input.IsNull) return SqlChars.Null;
        if (pattern.IsNull || replacement.IsNull) return input;
        int s;
        if (start.IsNull) s = 0;
        else s = start.Value - 1;
        if (s >= input.Value.Length)
            throw new ArgumentException(string.Format("RXREPLACE: Start expected to be between 1 and {0:G} (got {1:G}", input.Value.Length, s + 1));
        string _input = new string(input.Value).Substring(s);
        string _pattern = new string(pattern.Value);
        string _replacement = new string(replacement.Value);
        return new SqlChars(Regex.Replace(_input, _pattern, _replacement));
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlChars RXMATCH(SqlChars input, SqlChars pattern, SqlInt32 start, SqlInt32 n)
    {
        ValidateKey();
        if (input.IsNull || pattern.IsNull) return SqlChars.Null;
        int s, i;
        if (start.IsNull) s = 0;
        else s = start.Value - 1;
        if (s >= input.Value.Length)
            throw new ArgumentException(string.Format("RXMATCH: Start expected to be between 1 and {0:G} (got {1:G}", input.Value.Length, s + 1));
        if (n.IsNull) i = 0;
        else i = n.Value - 1;
        string _input = new string(input.Value).Substring(s);
        string _pattern = new string(pattern.Value);
        MatchCollection mc = Regex.Matches(_input, _pattern);
        if (mc.Count <= i) return SqlChars.Null;
        string _result = mc[i].Value;
        return new SqlChars(_result);
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 RXINDEX(SqlChars input, SqlChars pattern, SqlInt32 start, SqlInt32 n)
    {
        ValidateKey();
        if (input.IsNull || pattern.IsNull) return SqlInt32.Null;
        int s, i;
        if (start.IsNull) s = 0; 
        else s = start.Value - 1;
        if (s >= input.Value.Length)
            throw new ArgumentException(string.Format("RXINDEX: Start expected to be between 1 and {0:G} (got {1:G}", input.Value.Length, s + 1));
        if (n.IsNull) i = 0; 
        else i = n.Value - 1;
        string _input = new string(input.Value).Substring(s);
        string _pattern = new string(pattern.Value);
        MatchCollection mc = Regex.Matches(_input, _pattern);
        if (mc.Count <= i) return SqlInt32.Null;
        int _result = mc[i].Index;
        return _result + s + 1;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 RXLASTINDEX(SqlChars input, SqlChars pattern, SqlInt32 start)
    {
        ValidateKey();
        if (input.IsNull || pattern.IsNull) return SqlInt32.Null;
        int s;
        if (start.IsNull || start > input.Value.Length) s = input.Value.Length;
        else s = start.Value - 1;
        if (s >= input.Value.Length)
            throw new ArgumentException(string.Format("RXLASTINDEX: Start expected to be between 1 and {0:G} (got {1:G}", input.Value.Length, s + 1));
        string _input = new string(input.Value).Substring(0, s);
        string _pattern = new string(pattern.Value);
        MatchCollection mc = Regex.Matches(_input, _pattern);
        if (mc.Count == 0) return SqlInt32.Null;
        int _result = mc[mc.Count - 1].Index;
        return _result + 1;
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlBoolean RXISMATCH(SqlChars input, SqlChars pattern, SqlInt32 start)
    {
        ValidateKey();
        if (input.IsNull || pattern.IsNull) return SqlBoolean.Null;
        int s;
        if (start.IsNull) s = 0;
        else s = start.Value - 1;
        if (s >= input.Value.Length)
            throw new ArgumentException(string.Format("RXISMATCH: Start expected to be between 1 and {0:G} (got {1:G}", input.Value.Length, s + 1));
        string _input = new string(input.Value).Substring(s);
        string _pattern = new string(pattern.Value);
        return new SqlBoolean(Regex.IsMatch(_input, _pattern));
    }

    [SqlFunction(SystemDataAccess = SystemDataAccessKind.Read)]
    public static SqlInt32 RXMATCHCOUNT(SqlChars input, SqlChars pattern, SqlInt32 start)
    {
        ValidateKey();
        if (input.IsNull || pattern.IsNull) return SqlInt32.Null;
        int s;
        if (start.IsNull) s = 0;
        else s = start.Value - 1;
        if (s >= input.Value.Length)
            throw new ArgumentException(string.Format("RXMATCHCOUNT: Start expected to be between 1 and {0:G} (got {1:G}", input.Value.Length, s + 1));
        string _input = new string(input.Value).Substring(s);
        string _pattern = new string(pattern.Value);
        MatchCollection mc = Regex.Matches(_input, _pattern);
        return mc.Count;
    }

    [SqlFunction(FillRowMethodName = "ParseMatchRow", TableDefinition = "[index] INT, [length] INT, [value] NVARCHAR(MAX)", SystemDataAccess = SystemDataAccessKind.Read)]
    public static System.Collections.IEnumerable RXMATCHES(SqlChars input, SqlChars pattern)
    {
        ValidateKey();
        if (input.IsNull || pattern.IsNull) return new Match[] {};
        string _input = new string(input.Value);
        string _pattern = new string(pattern.Value);
        MatchCollection mc = Regex.Matches(_input, _pattern);
        Match[] ma = new Match[mc.Count];
        mc.CopyTo(ma, 0);
        return ma;
    }

    //--- Define the call-back function that will parse each row in the enumeration ---
    public static void ParseMatchRow(object item, out int index, out int length, out string value)
    {
        Match m = (Match)item;
        index = m.Index + 1;
        length = m.Length;
        value = m.Value;
    }

    #endregion

}