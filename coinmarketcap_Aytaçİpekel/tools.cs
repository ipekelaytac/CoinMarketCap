using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Net;
using System.IO;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

using System.Windows.Forms;

namespace coinmarketcap_Aytaçİpekel
{
    public class tools
    {
        public string lasterror = "";
        public string lastDBerror = "";

        public frmMain frmMain;

        public string WebRequestIste(string adres)
        {
            string sonuc = "";
            try
            {
                HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(adres);
                httpWebRequest.Method = WebRequestMethods.Http.Get;
                httpWebRequest.Accept = "application/json";

                var response = (HttpWebResponse)httpWebRequest.GetResponse();

                using (var sr = new StreamReader(response.GetResponseStream()))
                {
                    sonuc = sr.ReadToEnd();

                }

            }
            catch (Exception)
            {
            }

            return sonuc;

        }

        public static void bekle(int sure)
        {
            DateTime t = DateTime.Now.AddMilliseconds(sure);
            int i = 0;
            while (DateTime.Now < t)
            {
                if (i % 2 == 0)
                    Application.DoEvents();
                i++;
            }
        }

        public string UploadFileToFtp(string dosyaAdi, string ftpServerIP, string ftpServerUser, string ftpServerPass)
        {

            try
            {
                string filename = Path.GetFileName(dosyaAdi);

                FtpWebRequest ftp = (FtpWebRequest)FtpWebRequest.Create("ftp://" + ftpServerIP + "/" + filename);
                ftp.Credentials = new NetworkCredential(ftpServerUser, ftpServerPass);

                ftp.KeepAlive = true;
                ftp.UseBinary = true;
                ftp.Method = WebRequestMethods.Ftp.UploadFile;

                FileStream fs = File.OpenRead(dosyaAdi);
                byte[] buffer = new byte[fs.Length];
                fs.Read(buffer, 0, buffer.Length);
                fs.Close();

                Stream ftpstream = ftp.GetRequestStream();
                ftpstream.Write(buffer, 0, buffer.Length);
                ftpstream.Close();
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            return "";
        }
       


        public string UploadFileToFtp2(string dosyaAdi, string ftpServerIP, string ftpServerUser, string ftpServerPass)
        {

            FileInfo dosyaBilgisi = new FileInfo(dosyaAdi);
            string uri = "ftp://" + ftpServerIP + "/" + dosyaBilgisi.Name;
            FtpWebRequest ftpIstegi;

            ftpIstegi = (FtpWebRequest)FtpWebRequest.Create(new Uri(
                      "ftp://" + ftpServerIP + "/" + dosyaBilgisi.Name));

            ftpIstegi.Credentials = new NetworkCredential(ftpServerUser, ftpServerPass);

            // Baglantiyi sürekli açik tutuyor.
            ftpIstegi.KeepAlive = false;

            // Yapilacak islem (Upload)
            ftpIstegi.Method = WebRequestMethods.Ftp.UploadFile;

            //Verinin gönderim sekli.
            ftpIstegi.UseBinary = true;
            ftpIstegi.Timeout = 180 * 1000;

            //Sunucuya gönderilecek dosya uzunlugu bilgisi
            ftpIstegi.ContentLength = dosyaBilgisi.Length;

            // Buffer uzunlugu 2048 byte
            int bufferUzunlugu = 2048;
            byte[] buff = new byte[10000000];
            int sayi;

            FileStream stream = dosyaBilgisi.OpenRead();

            Stream str = null;
            try
            {
                str = ftpIstegi.GetRequestStream();

                sayi = stream.Read(buff, 0, bufferUzunlugu);

                while (sayi != 0)
                {
                    str.Write(buff, 0, sayi);
                    sayi = stream.Read(buff, 0, bufferUzunlugu);
                }
                return "";

                str.Close();
                stream.Close();
            }
            //ftpIstegi.Cl
            catch (Exception ex)
            {
                //str.Close();
                stream.Close();
                return ex.Message;
            }
            ftpIstegi = null;
        }



        public string DownloadFile(string URL, String Location)
        {
            string dosya = GetFileNameFromURL(URL);



            string uzanti = Path.GetExtension(dosya);
            string dosyaBas = dosya.Substring(0, dosya.Length - uzanti.Length);

            string dosyaYeni = dosyaBas + "_" + Rastgele(8).ToString() + uzanti;

            //dosya = dos


            WebClient webClient = new WebClient();
            try
            {
                webClient.DownloadFile(URL, Location + dosyaYeni);
            }
            catch (Exception)
            {
            }


            return Location + dosyaYeni;
        }

        public int Rastgele(int kac)
        {

            Random generator = new Random();
            bekle(100);
            generator = new Random();
            int r = generator.Next((int)Math.Pow(10, kac - 1), (int)Math.Pow(10, kac) - 1);

            return r;
        }

        public String GetFileNameFromURL(String hrefLink)
        {
            return Path.GetFileName(Uri.UnescapeDataString(hrefLink).Replace("/", "\\"));
        }

        public void DownloadFileShort(string URL, String Location)
        {
            WebClient webClient = new WebClient();
            webClient.DownloadFile(URL, Location);
        }

        public bool IsNullOrWhiteSpace(string value)
        {
            if (value == null)
                return true;
            return string.IsNullOrEmpty(value.Trim());
        }
        public string removeNulls(string text)
        {
            if (string.IsNullOrEmpty(text))
                text = " ";
            return text.Replace("'", "`");
        }

        public void logWriter(string txt)
        {
            try
            {
                frmMain.Invoke((MethodInvoker)delegate
                {
                    if (frmMain.log.Text.Length > 5000)
                        frmMain.log.Text = String.Empty;

                    frmMain.log.Text += DateTime.Now.ToString("dd MMM") + " - " + DateTime.Now.ToString("HH:mm:ss") + "  --  " + txt + Environment.NewLine;
                    frmMain.log.SelectionStart = frmMain.log.TextLength;
                    frmMain.log.ScrollToCaret();
                });
            }
            catch (Exception ex) { }

        }

        public bool ismyInteger(string value)
        {
            try
            {
                Int64 av = Convert.ToInt64(value);
                return true;
            }
            catch
            {
                return false;
            }
            return true;
        }


        public bool numeric_control(string number)
        {
            System.Text.RegularExpressions.Regex regex = new System.Text.RegularExpressions.Regex("[0-9]");

            if (regex.IsMatch(number))
                return true;
            else
                return false;
        }

        public static string ReverseString(string s)
        {
            char[] arr = s.ToCharArray();
            Array.Reverse(arr);
            return new string(arr);
        }

        public void TXT_Kaydet(string query)
        {
            StreamWriter swrt = new StreamWriter("query.txt");
            swrt.Write(query);
            swrt.Close();
        }

        public  static void KillMe()
        {
            /*
            DialogResult exitDialog = new DialogResult();
            exitDialog = MessageBox.Show("Programı kapatırsanız Vodafone Hizmet Sorgulama modülü çalışmaz!\r\nÇıkmak istediğinizden emin misiniz?", "TEKNOLOJİK BİLİŞİM", MessageBoxButtons.YesNo);
            if (exitDialog == DialogResult.Yes)
                */
            System.Diagnostics.Process.GetCurrentProcess().Kill();
        }


    }
}
