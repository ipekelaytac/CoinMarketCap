using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace coinmarketcap_Aytaçİpekel
{
    
    public class DAL
    {
       
        
        public frmMain frmMain;
        public DateTime lastQueryTime;
        public String lastQuerySQL;
        public SqlConnection myConnection;
       

        public DataSet CommandExecuteReader(String sql, SqlConnection conn) // parametresiz sql kullanımı
        {
            lastQueryTime = DateTime.Now;
            lastQuerySQL = sql;
            //frmMain.myTools.lastDBerror = "";
            DataSet ds = new DataSet();
            try
            {
                SqlCommand myCommand = new SqlCommand(sql, conn);
                myCommand.CommandTimeout = 600;

                SqlDataAdapter dataAdapter = new SqlDataAdapter(myCommand);

                dataAdapter.Fill(ds);
            }
            catch (Exception exp)
            {
                //frmMain.myTools.lastDBerror = exp.Message;
            }
            finally
            {
            }

            return ds;
        }


        public SqlDataReader CommandExecuteSQLReader(String sql, SqlConnection conn) // parametresiz sql kullanımı
        {

            lastQueryTime = DateTime.Now;
            lastQuerySQL = sql;
            //frmMain.myTools.lastDBerror = "";
            //Tracer.WriteLine("Dal.CommandExecuteReader", strCommand, new System.Diagnostics.StackTrace());

            int errC = 0;

            SqlDataReader r;
            try
            {
                SqlCommand myCommand = new SqlCommand(sql, conn);
                myCommand.CommandTimeout = 600;

                r = myCommand.ExecuteReader();

                errC = -1;

                return r;

            }
            catch (Exception exp)
            {
                frmMain.myTools.lastDBerror = exp.Message;
            }
            finally
            {
            }


            return null;
        }


        public string CommandExecuteSQLScalar(String sql, SqlConnection conn) // parametresiz sql kullanımı
        {

            lastQueryTime = DateTime.Now;
            lastQuerySQL = sql;
            //frmMain.myTools.lastDBerror = "";
            //Tracer.WriteLine("Dal.CommandExecuteReader", strCommand, new System.Diagnostics.StackTrace());

            int errC = 0;

            string r;
            try
            {
                SqlCommand myCommand = new SqlCommand(sql, conn);
                myCommand.CommandTimeout = 600;

                r = myCommand.ExecuteScalar().ToString();

                errC = -1;

                return r;

            }
            catch (Exception exp)
            {
                //frmMain.myTools.lastDBerror = exp.Message;
            }
            finally
            {
            }


            return null;
        }

        public void CommandExecuteNonQuery(String sql, SqlConnection conn)
        {
            lastQueryTime = DateTime.Now;
            lastQuerySQL = sql;
           // frmMain.myTools.lastDBerror = "";
            //Tracer.WriteLine("Dal.CommandExecuteNonQuery", strCommand, new System.Diagnostics.StackTrace());
            try
            {
                SqlCommand myCommand = new SqlCommand(sql, conn);
                myCommand.CommandTimeout = 600;
                myCommand.ExecuteNonQuery();
            }
            catch (Exception exp)
            {
                lastQuerySQL = sql;
                //frmMain.myTools.lastDBerror = exp.Message;
            }
            finally
            {
            }
        }


        public void OpenSQLConnection(String ConnectionString, SqlConnection conn) 
        {
            myConnection  =new  SqlConnection(ConnectionString);

            ConControl(myConnection);
            
        }



        public void ConControl(SqlConnection c)
        {
            if (c.State != ConnectionState.Open)
            {
                c.Open();
            }
        }





    }
}
