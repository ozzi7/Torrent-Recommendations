using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Text.RegularExpressions;

namespace TorrentRecommendation
{
    static class Program   
    {
        static public bool IP_BASED = false; // IP based predictions ignore port numbers
        static public int MAX_PEERS_FOR_MAE = Int32.MaxValue;
        public static List<int> mapping;

        static void Main(string[] args)
        {
            CollaborativeFiltering cF = new CollaborativeFiltering();

            Console.Write("\nEnter 1 for quality measure or 2 for Webserver: ");
            string input = Console.ReadLine();
            int choice = -1;
            bool result = Int32.TryParse(input, out choice);
            if (!result)
                choice = 1;

            if (choice == 2)
            {
                while (true)
                {
                    IP_BASED = true;
                    var txtFiles = Directory.EnumerateFiles(Directory.GetCurrentDirectory(), "*Request.txt", SearchOption.AllDirectories);

                    foreach (string reqFile in txtFiles)
                    {
                        string fileName = reqFile.Substring(Directory.GetCurrentDirectory().Length + 1);

                        string[] requestUrls = File.ReadAllLines(reqFile);
                        string ip = "";

                        Regex reg = new Regex(@"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(Request)");
                        MatchCollection matches = reg.Matches(fileName);
                        if (matches.Count == 1)
                        {
                            ip = matches[0].Groups[1].Value;

                            System.IO.StreamReader file = new System.IO.StreamReader(fileName);
                            List<string> kickAssUrls = new List<string>();
                            string line;
                            while ((line = file.ReadLine()) != null)
                                kickAssUrls.Add(line);

                            file.Close();
                            File.Delete(reqFile);

                            List<Tuple<int, double>> predictions = cF.GetPredictionFast(kickAssUrls, ip);

                            predictions.Sort((a, b) => b.Item2.CompareTo(a.Item2));
                            File.Create(ip + "Response.txt").Dispose();
                            TextWriter tw = new StreamWriter(ip + "Response.txt");
                            foreach (Tuple<int, double> t in predictions)
                            {
                                Tuple<string, string, string, string> tuple = cF.GetTorrentDataFromMergeID(t.Item1);
                                tw.WriteLine(t.Item2 + " " + tuple.Item1 + " " + tuple.Item2 + " " + tuple.Item3 + " " + tuple.Item4);
                            }
                            tw.Close();
                        }
                    }
                }
            }
            else
            {
                // Measure recommendation quality
                //cF.CalculateMeanAbsoluteError();
                cF.CalculateRankEvaluation();
                Console.Read();

                // Or show some predictions
                cF.ShowFirstPredictions();
                Console.Read();
            }
        }
    }
}
