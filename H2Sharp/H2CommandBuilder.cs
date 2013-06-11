using System;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.H2;
using System.Text.RegularExpressions;
using System.Data;

namespace System.Data.H2
{
    /// <summary>
	/// This command builder is still buggy, please only use it to debug it :-)
	/// </summary>
	public class H2CommandBuilder : DbCommandBuilder
    {
        //H2Connection connection;
        static readonly Regex selectRegex = new Regex("^select\\s+(.*)\\s+from\\s+([^\\s]+?)(?:\\s+where\\s+(?:.*))?(?:\\s+order\\s+by\\s+(?:.*))?$", RegexOptions.Multiline | RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static readonly Regex columnRegex = new Regex("\"(.*)\"", RegexOptions.Compiled | RegexOptions.Multiline);
        
        public H2CommandBuilder(H2DataAdapter adapter)
        {
            DataAdapter = adapter;
            //Letting ADO.NET do its job does not appear to work (yet) :
            if (false)
            {
                adapter.InsertCommand = (H2Command)GetInsertCommand();
                adapter.UpdateCommand = (H2Command)GetUpdateCommand();
                return;
            }

            var connection = adapter.SelectCommand.Connection;
            var select = adapter.SelectCommand.CommandText.ToLower();
            var mat = selectRegex.Match(select);
            if (!mat.Success)
                throw new Exception("Select command not recognized : '" + select + "'");

            var tableName = mat.Groups[2].Value;
            {
                var mmat = columnRegex.Match(tableName);
                if (mmat.Success)
                    tableName = mmat.Groups[1].Value;
            }

			var columnTypeCodes = connection.GetColumnTypeCodes(tableName);

            IList<String> cols = mat.Groups[1].Value.Split(',');
            if (cols.Count == 1 && cols[0].Trim().Equals("*"))
                cols = columnTypeCodes.Keys.ToList();
			
			cols = cols.Select(c => c.Trim()).ToList();

            var updateCommand = new H2Command(connection);
            var insertCommand = new H2Command(connection);
            var updateSets = new List<String>();
            var updateWheres = new List<String>();
            //var namesUp = new List<String>();
            //var valuesUp = new List<String>();
            var colasrx = new Regex("\"?(.*)\"? as \"?(.*)\"?");
            int nextParam = 0;
            var aliases = new Dictionary<String, String>();
            foreach (var col in cols)
            {
                var colasmat = colasrx.Match(col);
                String alias;
                String columnName;
                if (colasmat.Success)
                {
                    alias = colasmat.Groups[2].Value.ToUpper().Trim();
                    columnName = colasmat.Groups[1].Value.ToUpper().Trim();
                }
                else
                {
                    alias = columnName = col.ToUpper().Trim();
                }
                aliases[columnName] = alias;
                var paramName = (nextParam++).ToString();

                updateSets.Add("\"" + columnName + "\" = ?");//:" + paramName);

				var typeCode = columnTypeCodes[columnName];
				var dbType = H2Helper.GetDbType(typeCode);
                updateCommand.Parameters.Add(new H2Parameter(paramName, dbType)
                {
                    SourceColumn = alias,
                    DbType = dbType,
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Current
                });

            }
			var pks = connection.GetPrimaryKeysColumns(tableName);
            foreach (var pk in pks.Select(c => c.ToUpper()))
            {
                var columnName = pk;
                var paramName = (nextParam++).ToString();
                updateWheres.Add("\"" + columnName + "\" = ?");//:" + paramName);

                String alias;
                if (!aliases.TryGetValue(columnName, out alias))
                    alias = columnName;
                
				var typeCode = columnTypeCodes[columnName];
				var dbType = H2Helper.GetDbType(typeCode);
                updateCommand.Parameters.Add(new H2Parameter(paramName, dbType)
                {
                    SourceColumn = alias,
                    DbType = dbType,
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Original
                });
            }
            var insertValues = new List<String>();
            nextParam = 0;
            foreach (var columnName in cols.Select(c => c.ToUpper()))
            {
                var paramName = (nextParam++).ToString();
                insertValues.Add("?");//":" + paramName);
                String alias;
                if (!aliases.TryGetValue(columnName, out alias))
                    alias = columnName;
                
				var typeCode = columnTypeCodes[columnName];
				var dbType = H2Helper.GetDbType(typeCode);
                insertCommand.Parameters.Add(new H2Parameter(paramName, dbType)
                {
                    SourceColumn = alias,
					DbType = dbType,
                    Direction = ParameterDirection.Input,
                    SourceVersion = DataRowVersion.Original
                });
            }
            updateCommand.CommandText = "update " + tableName + " set " + updateSets.Commas() + " where " + updateWheres.Commas();
            adapter.UpdateCommand = updateCommand;
            insertCommand.CommandText = "insert into " + tableName + "(" + cols.Commas() + ") values (" + insertValues.Commas() + ")";
            adapter.InsertCommand = insertCommand;
            
        }

        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
            parameter.DbType = (DbType)row["DbType"];
        }

        protected override string GetParameterName(string parameterName)
        {
            return parameterName;
        }

        protected override string GetParameterName(int parameterOrdinal)
        {
            return "param" + parameterOrdinal;
        }

        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            return "?";
        }

        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            //throw new NotImplementedException();
        }
    }
	public static class ConnectionExtensions
    {
        public static List<String> ReadStrings(this H2Connection connection, String query)
        {
            var ret = new List<String>();
            var reader = new H2Command(query, connection).ExecuteReader();
            while (reader.Read())
                ret.Add(reader.GetString(0));
            return ret;
        }
        public static DataTable ReadTable(this H2Connection connection, String tableName)
        {
            if (tableName == null)
                return null;
            return connection.ReadQuery("select * from \"" + tableName + "\"");
        }
        public static DataTable ReadQuery(this H2Connection connection, String query)
        {
            if (query == null)
                return null;
            var table = new DataTable()
            {
                CaseSensitive = false
            };
            new H2DataAdapter(new H2Command(query, connection)).Fill(table);
            return table;
        }
        public static String ReadString(this H2Connection connection, String query)
        {
            var result = new H2Command(query, connection).ExecuteScalar() as String;
            return result;
        }
        public static Dictionary<String, T> ReadMap<T>(this H2Connection connection, String query)
        {
            var ret = new Dictionary<String, T>();
            var reader = new H2Command(query, connection).ExecuteReader();
            while (reader.Read())
            {
                var key = reader.GetString(0);
                var value = reader.GetValue(1);
                if (value == DBNull.Value)
                    ret[key] = default(T);
                else
                    ret[key] = (T)value;
            }
            return ret;
        }
    }
	public static class CollectionExtensions
    {
        public static T[] Array<T>(params T[] a)
        {
            return a;
        }
        public static String Commas<T>(this IEnumerable<T> col)
        {
            return col.Implode(", ");
        }
        public static String Implode<T>(this IEnumerable<T> col, String sep)
        {
            return col.Where(e => e != null).Select(e => e.ToString()).Aggregate((a, b) => a + sep + b);
        }
    }
}
