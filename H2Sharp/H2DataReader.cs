#region MIT License
/*
 * Copyright © 2008 Jonathan Mark Porter.
 * H2Sharp is a wrapper for the H2 Database Engine. http://h2sharp.googlecode.com
 * 
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
#endregion

using System.Data.Common;
using java.sql;
using System.Collections.Generic;


namespace System.Data.H2
{


    public sealed class H2DataReader : DbDataReader
    {
        private static int ConvertOrdnal(int ordinal)
        {
            if (ordinal == int.MaxValue) { throw new H2Exception("invalid ordinal"); }
            return ordinal+1;
        }

		private H2Connection connection;
        private ResultSet set;
        private ResultSetMetaData meta;

        internal H2DataReader(H2Connection connection, ResultSet set)
        {
            this.set = set;
			this.connection = connection;
        }


        private ResultSetMetaData Meta
        {
            get
            {
                if (meta == null)
                {
                    meta = set.getMetaData();
                }
                return meta;
            }
        }
        public override bool IsDBNull(int ordinal)
        {
            return set.getObject(ConvertOrdnal(ordinal)) == null;
        }
        public override bool NextResult()
        {
            throw new NotImplementedException();
        }
        public override int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }
        public override bool HasRows
        {
            get { throw new NotImplementedException(); }
        }
        public override bool IsClosed
        {
            get { return set.isClosed(); }
        }
        public override object this[string name]
        {
            get { return set.getObject(name); }
        }
        public override object this[int ordinal]
        {
            get { return set.getObject(ConvertOrdnal(ordinal)); }
        }
        public override int Depth
        {
            get { return Meta.getColumnCount(); }
        }
        public override int FieldCount
        {
            get { return Meta.getColumnCount(); }
        }


        public override bool GetBoolean(int ordinal)
        {
            return set.getBoolean(ConvertOrdnal(ordinal));
        }
        public override byte GetByte(int ordinal)
        {
            return set.getByte(ConvertOrdnal(ordinal));
        }
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            Byte[] rv = set.getBytes(ConvertOrdnal(ordinal));
            Array.Copy(rv, dataOffset, buffer, bufferOffset, length);
            return length;
        }
        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }
        public override string GetDataTypeName(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override DateTime GetDateTime(int ordinal)
        {

            return UTCStart.AddMilliseconds(set.getDate(ordinal).getTime());
        }
        static readonly DateTime UTCStart = new DateTime(1970, 1, 1);
        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override double GetDouble(int ordinal)
        {
            return set.getDouble(ConvertOrdnal(ordinal));
        }
        public override System.Collections.IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        Type[] types;
        public override Type GetFieldType(int ordinal)
        {
            if (types == null)
                types = new Type[Meta.getColumnCount() + 1];

            var type = types[ordinal];
            if (type == null)
                types[ordinal] = type = DoGetFieldType(ordinal);
            return type;
        }
        Type DoGetFieldType(int ordinal)
        {
            int typeCode = Meta.getColumnType(ConvertOrdnal(ordinal));
            return H2Helper.GetType(typeCode);
        }
        public override float GetFloat(int ordinal)
        {
            return set.getFloat(ConvertOrdnal(ordinal));
        }
        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }
        public override short GetInt16(int ordinal)
        {
            return set.getShort(ConvertOrdnal(ordinal));
        }
        public override int GetInt32(int ordinal)
        {
            return set.getInt(ConvertOrdnal(ordinal));
        }
        public override long GetInt64(int ordinal)
        {
            return set.getLong(ConvertOrdnal(ordinal));
        }
        public override string GetName(int ordinal)
        {
            var i = ConvertOrdnal(ordinal);
            var s = Meta.getColumnLabel(i);
            return s == null ? Meta.getColumnName(i) : s;
        }
        public override int GetOrdinal(string name)
        {
            for (int index = 0; index < Meta.getColumnCount(); ++index)
            {
                if (Meta.getColumnName(index) == name)
                {
                    return index;
                }
            }
            return -1;
        }
        public override DataTable GetSchemaTable()
        {
			/*
			JDBC reference :
			http://java.sun.com/j2se/1.5.0/docs/api/java/sql/ResultSetMetaData.html
			
			ADO.NET reference :
			http://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqldatareader.getschematable.aspx
			*/
			var table = new DataTable();
			var ColumnName = table.Columns.Add("ColumnName", typeof(String));
			var ColumnOrdinal = table.Columns.Add("ColumnOrdinal", typeof(Int32));
			var ColumnSize = table.Columns.Add("ColumnSize", typeof(Int32));
			var NumericPrecision = table.Columns.Add("NumericPrecision", typeof(Int32));
			var NumericScale = table.Columns.Add("NumericScale", typeof(Int32));
			var IsUnique = table.Columns.Add("IsUnique", typeof(bool));
			var IsKey = table.Columns.Add("IsKey", typeof(bool));
			var BaseServerName = table.Columns.Add("BaseServerName", typeof(String));
			var BaseCatalogName = table.Columns.Add("BaseCatalogName", typeof(String));
			var BaseColumnName = table.Columns.Add("BaseColumnName", typeof(String));
			var BaseSchemaName = table.Columns.Add("BaseSchemaName", typeof(String));
			var BaseTableName = table.Columns.Add("BaseTableName", typeof(String));
			var DataType = table.Columns.Add("DataType", typeof(Type));
			var AllowDBNull = table.Columns.Add("AllowDBNull", typeof(bool));
			var ProviderType = table.Columns.Add("ProviderType");
			var IsAliased = table.Columns.Add("IsAliased", typeof(bool));
			var IsExpression = table.Columns.Add("IsExpression", typeof(bool));
			var IsIdentity = table.Columns.Add("IsIdentity", typeof(bool));
			var IsAutoIncrement = table.Columns.Add("IsAutoIncrement", typeof(bool));
			var IsRowVersion = table.Columns.Add("IsRowVersion", typeof(bool));
			var IsHidden = table.Columns.Add("IsHidden", typeof(bool));
			var IsLong = table.Columns.Add("IsLong", typeof(bool));
			var IsReadOnly = table.Columns.Add("IsReadOnly", typeof(bool));
			var ProviderSpecificDataType = table.Columns.Add("ProviderSpecificDataType");
			var DataTypeName = table.Columns.Add("DataTypeName", typeof(String));
            var DbType = table.Columns.Add("DbType", typeof(DbType)); // not standard !!!
			//var XmlSchemaCollectionDatabase = table.Columns.Add("XmlSchemaCollectionDatabase");
			//var XmlSchemaCollectionOwningSchema = table.Columns.Add("XmlSchemaCollectionOwningSchema");
			//var XmlSchemaCollectionName = table.Columns.Add("XmlSchemaCollectionName");
			
			//var dbMeta = connection.connection.getMetaData();
			var tablesPksAndUniques = new Dictionary<String, KeyValuePair<HashSet<String>, HashSet<String>>>();
			var meta = Meta;
			
			var nCols = meta.getColumnCount();
			table.MinimumCapacity = nCols;
			for (int iCol = 1; iCol <= nCols; iCol++) 
			{
				// Beware : iCol starts at 1 (JDBC convention)
				var row = table.NewRow();
				var name = meta.getColumnName(iCol);	
				var label = meta.getColumnLabel(iCol);
				var tableName = meta.getTableName(iCol);
				
				KeyValuePair<HashSet<String>, HashSet<String>> pksAndUniques;
				if (!tablesPksAndUniques.TryGetValue(tableName, out pksAndUniques)) {
					pksAndUniques = new KeyValuePair<HashSet<string>, HashSet<string>>(
						connection.GetPrimaryKeysColumns(tableName),
						connection.GetUniqueColumns(tableName)
					);
				}
				
				row[ColumnName] = label != null ? label : name;
				row[ColumnOrdinal] = iCol - 1;
				row[BaseColumnName] = name;
				row[BaseSchemaName] = meta.getSchemaName(iCol);
				row[BaseTableName] = tableName;
				row[	ColumnSize] = meta.getColumnDisplaySize(iCol);
				row[IsReadOnly] = meta.isReadOnly(iCol);
				row[IsKey] = pksAndUniques.Key.Contains(name);
				row[IsUnique] = pksAndUniques.Value.Contains(name);
				row[DataTypeName] = meta.getColumnTypeName(iCol); // TODO check this !
				row[NumericPrecision] = meta.getPrecision(iCol);
				row[NumericScale] = meta.getScale(iCol);
				var jdbcType = meta.getColumnType(iCol);
				var type = H2Helper.GetType(jdbcType);
                var dbType = H2Helper.GetDbType(jdbcType);
                row[DataType] = type;
                row[DbType] = dbType;
				row[AllowDBNull] = meta.isNullable(iCol);
				table.Rows.Add(row);
			}
			return table;
            //throw new NotImplementedException();
        }
        public override string GetString(int ordinal)
        {
            return set.getString(ConvertOrdnal(ordinal));
        }


        H2Helper.Converter[] converters;

        public override object GetValue(int ordinal)
        {
            var convOrd = ConvertOrdnal(ordinal);
            object result = set.getObject(convOrd);
            if (result == null)
                return DBNull.Value;

            if (converters == null)
                converters = new H2Helper.Converter[Meta.getColumnCount()];

            H2Helper.Converter converter = converters[ordinal];
            if (converter == null)
                converters[ordinal] = converter = H2Helper.ConverterToCLR(Meta.getColumnType(convOrd));

            return converter(result);
        }
        public override int GetValues(object[] values)
        {
            if (values == null) { throw new ArgumentNullException("values"); }
            for (int index = 0; index < values.Length; ++index)
            {
                values[index] = GetValue(index);
            }
            return values.Length;
        }
        public override bool Read()
        {
            return set.next();
        }
        public override void Close()
        {
            set.close();
        }

    }
	static class DatabaseMetaDataExtensions {
		
		public static Dictionary<String, int> GetColumnTypeCodes(this H2Connection connection, String tableName) {
			// Reference : http://java.sun.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
			/*try {
				var dbMeta = connection.connection.getMetaData();
				var res = dbMeta != null ? dbMeta.getColumns(null, null, tableName, null) : null;
				if (res != null) {
					var ret = new Dictionary<String, int>();
					while (res.next()) {
						var columnName = res.getString(4);
						var colType = res.getInt(5);
						ret[columnName] = colType;
					}
					return ret;
				}
			} catch (Exception ex) { 
				Console.WriteLine(ex);
			}*/
			return connection.ReadMap<int>("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where upper(table_name) = '" + tableName.ToUpper() + "'");
		}		
		public static HashSet<String> GetPrimaryKeysColumns(this H2Connection connection, String tableName) {
			// Reference : http://java.sun.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
			/*try {
				var dbMeta = connection.connection.getMetaData();
				var res = dbMeta != null ? dbMeta.getPrimaryKeys(null, null, tableName) : null;
				if (res != null) {
					var ret = new HashSet<String>();
					while (res.next()) {
						var columnName = res.getString(4);
						ret.Add(columnName);
					}
					return ret;
				}
			} catch (Exception ex) { 
				Console.WriteLine(ex);
			}*/
			var ret = new HashSet<String>();
			foreach (var list in connection.ReadStrings("select column_list from INFORMATION_SCHEMA.CONSTRAINTS where constraint_type = 'PRIMARY KEY' and upper(table_name) = '" + tableName.ToUpper() + "' ")) {
				foreach (var col in list.Split(','))
					ret.Add(col.Trim());
			}
			return ret;
		}
		public static HashSet<String> GetUniqueColumns(this H2Connection connection, String tableName) {
			// Reference : http://java.sun.com/javase/6/docs/api/java/sql/DatabaseMetaData.html#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
			/*try {
				var dbMeta = connection.connection.getMetaData();
				var res = dbMeta != null ? dbMeta.getIndexInfo(null, null, tableName, true, false) : null;
				if (res != null) {
					var ret = new HashSet<String>();
					while (res.next()) {
						var columnName = res.getString(4);
						ret.Add(columnName);
					}
					return ret;
				}
			} catch (Exception ex) { 
				Console.WriteLine(ex);
			}*/
			return new HashSet<String>(connection.ReadStrings("select column_list from INFORMATION_SCHEMA.CONSTRAINTS where constraint_type = 'UNIQUE' and upper(table_name) = '" + tableName.ToUpper() + "'"));
		}	
	}
}