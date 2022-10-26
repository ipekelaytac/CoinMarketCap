using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Newtonsoft.Json;


namespace coinmarketcap_Aytaçİpekel
{
    public partial class frmMain : Form
    {
        public tools myTools = new tools();
        public DAL myDal = new DAL();
        public string constr = "";
        string simdikitarih = "";
        public frmMain()
        {
            InitializeComponent();
        }
        private static double ConvertDateTimeToTimestamp(DateTime value)
        {
            TimeSpan epoch = (value - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime());
            return (double)epoch.TotalSeconds;
        }
        private void frmMain_Load(object sender, EventArgs e)
        {
            myDal.frmMain = this;
            myTools.frmMain = this;
            myTools.logWriter("Robot Başladı");
            constr = System.Configuration.ConfigurationSettings.AppSettings["con"].ToString();
            myDal.OpenSQLConnection(constr, myDal.myConnection);
            var parca = ConvertDateTimeToTimestamp(DateTime.UtcNow).ToString().Split(',');
            simdikitarih = parca[0];
            DateTime dt = new DateTime(DateTime.Now.Year, DateTime.Now.Month, DateTime.Now.Day - 1, 2, 0, 0);
            simdikitarih = ConvertDateTimeToTimestamp(dt).ToString().Split('.')[0];
            timer1.Interval = 3000;
            timer1.Start();
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            timer1.Stop();
            button1_Click(null, null);
        }
        private void button1_Click(object sender, EventArgs e)
        {
            ParaListesiDoldur();
        }
        private void ParaListesiDoldur()
        {
            int sayac = 0;
            string url = "https://web-api.coinmarketcap.com/v1/cryptocurrency/listings/latest";
            string sonuc = myTools.WebRequestIste(url);
            dynamic dizi = JsonConvert.DeserializeObject(sonuc);
            string sql = "";
            string slug = "";
            string sembol = "";
            foreach (var item in dizi.data)
            {
                sembol = item.symbol;
                slug = item.slug;
                sql = $"select id, kisaltma from PARA (nolock) where kisaltma != 'USD' and (kaynak = 'CoinMarketCap' or kaynak is null) and kisaltma = '{sembol.ToUpper()}'";
                SqlDataReader oku = myDal.CommandExecuteSQLReader(sql, myDal.myConnection);
                while (oku.Read())
                {
                    DataGetir(Convert.ToInt32(oku[0]), slug, oku[1].ToString());
                    sayac++;
                }
                oku.Close();
                myTools.logWriter(sembol.ToUpper() + " kontrol ediliyor.");
                Application.DoEvents();
            }
            myTools.logWriter("Çekme işlemi bitti " + sayac + " veri çekildi.");
        }
        private void DataGetir(int cid, string sid, string sembol)
        {
            string son_data_tarihi = "";
            string url = "https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/historical?convert=USD&slug=" + sid + "&time_end=" + simdikitarih + "&time_start=1104710400";
            string sonuc = myTools.WebRequestIste(url);
            DataTable dt = new DataTable();
            dt.Columns.Add("tarih", typeof(DateTime));
            dt.Columns.Add("para_id", typeof(int));
            dt.Columns.Add("fiyat", typeof(decimal));
            string sql = $"select top 1 tarih from DATA_GUNLUK_PARA where para_id = {cid} order by tarih desc";
            SqlDataReader oku = myDal.CommandExecuteSQLReader(sql, myDal.myConnection);
            while (oku.Read())
            {
                var parca = oku[0].ToString().Substring(0, 10).Split('.');
                son_data_tarihi = parca[2] + parca[1] + parca[0];
            }
            oku.Close();
            dynamic obj = JsonConvert.DeserializeObject(sonuc);
            foreach (var item in obj.data.quotes)
            {
                string tarih = item.time_close;
                string fiyat = item.quote.USD.close;
                var parca = tarih.Substring(0, 10).Split('/');
                string starih = parca[2] + parca[0] + parca[1];
                var p = DateTime.Now.ToString().Substring(0, 10).Split('.');
                string simdi_tarih = p[2] + p[1] + p[0];
                if (starih == simdi_tarih) continue;
                if (son_data_tarihi != "")
                {
                    if (Convert.ToInt32(son_data_tarihi) < Convert.ToInt32(starih))
                    {
                        DataRow dr = dt.NewRow();
                        dr["tarih"] = tarih;
                        dr["para_id"] = cid;
                        dr["fiyat"] = fiyat.ToString().Replace(".", ",");
                        dt.Rows.Add(dr);
                    }
                    else
                        continue;
                }
                else
                {
                    var t = tarih.Substring(0, 10).Split('/');
                    tarih = t[2] + "." + t[0] + "." + t[1];
                    DataRow dr = dt.NewRow();
                    dr["tarih"] = tarih;
                    dr["para_id"] = cid;
                    dr["fiyat"] = fiyat.ToString().Replace(".", ",");
                    dt.Rows.Add(dr);
                }
                Application.DoEvents();
            }
            BulkInsert(dt);
            myDal.CommandExecuteNonQuery($"update para set cekildi=1, kaynak='CoinMarketCap' where kisaltma = '{sembol}'", myDal.myConnection);
            myTools.logWriter("Para Eklendi: " + sembol);
        }
        private void BulkInsert(DataTable dt)
        {
            SqlConnection con = new SqlConnection(constr);
            SqlBulkCopy bulk = new SqlBulkCopy(con);
            bulk.DestinationTableName = "DATA_GUNLUK_PARA";
            con.Open();
            bulk.WriteToServer(dt);
            con.Close();
        }

    }
}
