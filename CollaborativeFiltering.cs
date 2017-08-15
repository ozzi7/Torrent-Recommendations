using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;

namespace TorrentRecommendation
{
    class CollaborativeFiltering
    {
        private List<List<int>> associationTable; // Holds mergeID -> peerID
        private List<List<int>> invertedAssociationTable; // Holds peerID -> mergeID
        private List<List<Tuple<int, double>>> itemSimilarityTable; // Item1 -> Item2 similarity (symmetrical, no identity)
        private List<double> lengthOfItemVectors;
        private List<double> sumOfSimilarityForTorrent;

        private List<Tuple<int, double>> predictions;
        private List<Tuple<int, double>> predictionsFast;
        private MySQL mySQL = new MySQL();

        public CollaborativeFiltering()
        {
            Program.mapping = new List<int>();

            associationTable = mySQL.GetAssociationTable();
            invertedAssociationTable = mySQL.GetInvertedAssociationTable();
            if (associationTable == null) return;
            GenerateItemSimilarityTable();
            CalculateSumOfSimilarityTable();
        }
        /// <summary>
        /// Cosine-based similarity - does not contain entries (i,i)
        /// How many shared peers needed to set similarity != 0?
        /// </summary>
        public void GenerateItemSimilarityTable()
        {
            // 2 parameters for fine tuning similarity calculations
            int minCommonPeersNeeded = 3;
            int ignoreTorrentsWithLessThanXPeers = 0;

            lengthOfItemVectors = Enumerable.Repeat(new double(), associationTable.Count).ToList();
            itemSimilarityTable = new List<List<Tuple<int, double>>>(associationTable.Count);
            for (int i = 0; i < associationTable.Count; ++i)
            {
                List<Tuple<int, double>> list = new List<Tuple<int, double>>();
                itemSimilarityTable.Add(list);
            }

            // Calculate vector length ||i|| for all items
            for (int item = 0; item < associationTable.Count; item++)
            {
                lengthOfItemVectors[item] = Math.Sqrt((double)associationTable[item].Count / (double)associationTable.Count);
            }

            // Calculate similarity sim(item1,item2) = i*j/ (||i||*||j||) for all torrents
            for (int item1 = 0; item1 < associationTable.Count; item1++)
            {
                Console.Write("\rCalculating torrent similarity table! " + Math.Round((double)(100 * (item1 + 1)) / (double)associationTable.Count, 2) + "%       ");
                for (int item2 = item1 + 1; item2 < associationTable.Count; item2++)
                {
                    if (associationTable[item2].Count >= ignoreTorrentsWithLessThanXPeers && associationTable[item1].Count >= ignoreTorrentsWithLessThanXPeers)
                    {
                        // Ignore all torrents with less than 100 peers!
                        double similarity = 0;
                        double dotProduct = 0;

                        int posItem2 = 0;
                        for (int posItem1 = 0; posItem1 < associationTable[item1].Count; posItem1++)
                        {
                            for (; posItem2 < associationTable[item2].Count; posItem2++)
                            {
                                if (associationTable[item1][posItem1] == associationTable[item2][posItem2])
                                {
                                    dotProduct++;
                                }
                                else if (associationTable[item1][posItem1] < associationTable[item2][posItem2])
                                {
                                    break;
                                }
                            }
                        }
                        if (lengthOfItemVectors[item1] != 0 && lengthOfItemVectors[item2] != 0 && dotProduct >= minCommonPeersNeeded)
                        {
                            similarity = dotProduct / (lengthOfItemVectors[item1] * lengthOfItemVectors[item2]);
                        }

                        if (similarity != 0)
                        {
                            itemSimilarityTable[item1].Add(Tuple.Create(item2, similarity));
                            itemSimilarityTable[item2].Add(Tuple.Create(item1, similarity));
                        }
                    }
                }
            }
        }
        private void CalculateSumOfSimilarityTable()
        {
            sumOfSimilarityForTorrent = new List<double>(new double[itemSimilarityTable.Count]);

            for(int i = 0; i < itemSimilarityTable.Count; ++i)
            {
                double sum = 0.0;
                for(int j = 0; j < itemSimilarityTable[i].Count;++j)
                {
                    sum += itemSimilarityTable[i][j].Item2;
                }
                sumOfSimilarityForTorrent[i] = sum;
            }
        }
        /// <summary>
        /// Checks what torrents the peer is downloading first
        /// Makes predictions even if peer has already downloaded torrent! (used for MAE)
        /// </summary>
        /// <param name="peerID"></param>
        /// <returns></returns>
        public List<Tuple<int, double>> GetPredictionFast(int peerID)
        {
            List<Tuple<int, double>> predictionVector = new List<Tuple<int, double>>();
            // Calculate a rating prediction for all torrents
            if (peerID < invertedAssociationTable.Count)
            {
                if (invertedAssociationTable[peerID].Count != 0)
                {
                    // We can't make predictions for peers without torrents
                    for (int torr1 = 0; torr1 < associationTable.Count; torr1++)
                    {
                        double weightedSum = 0;
                        // Go through all torrents downloaded by peerID
                        for (int torrIndex = 0; torrIndex < invertedAssociationTable[peerID].Count; ++torrIndex)
                        {
                            // Check if there is a similarity for the downloaded torrent and the one we want prediction of
                            int index = itemSimilarityTable[torr1].FindIndex(t => t.Item1 == invertedAssociationTable[peerID][torrIndex]);
                            if (index != -1)
                            {
                                // We found a neighbor torrent with a similarity > 0 and the peer has downloaded it
                                weightedSum += itemSimilarityTable[torr1][index].Item2;
                            }
                        }
                        if (sumOfSimilarityForTorrent[torr1] != 0 && weightedSum != 0)
                            predictionVector.Add(Tuple.Create(torr1, weightedSum / sumOfSimilarityForTorrent[torr1]));
                    }
                }
            }
            return predictionVector;
        }
        /// <summary>
        /// Get predictions for an IP (->IP based prediction) which in addition to the torrents in DB also
        /// downloaded specified kickassUrl torrents (Does not make predictions for downloaded torrents or kickassurls)
        /// </summary>
        /// <param name="kickassUrls"></param>
        /// <param name="ip"></param>
        /// <returns></returns>
        public List<Tuple<int, double>> GetPredictionFast(List<string> kickassUrls, string ip)
        {
            List<Tuple<int, double>> predictionVector = new List<Tuple<int, double>>();
            List<int> mergeIDs = mySQL.GetMergeIDFromKickassURL(kickassUrls);
            int mappedID = mySQL.GetMappedIDFromIP(ip);

            if (mappedID == -1)
            {
                // This IP is not in the database, use only kickassUrls
                for (int torr1 = 0; torr1 < associationTable.Count; torr1++)
                {
                    if (!mergeIDs.Contains(torr1))
                    {
                        // Dont make predictions for torr1 if it is one of the entered kickassurl ids
                        double weightedSum = 0;
                        // Go through all torrents downloaded by mergeID 
                        for (int torrIndex = 0; torrIndex < mergeIDs.Count; ++torrIndex)
                        {
                            // Check if there is a similarity for the downloaded torrent and the one we want prediction of
                            int index = itemSimilarityTable[torr1].FindIndex(t => t.Item1 == mergeIDs[torrIndex]);
                            if (index != -1)
                            {
                                // We found a neighbor torrent with a similarity > 0 and the peer has downloaded it
                                weightedSum += itemSimilarityTable[torr1][index].Item2;
                            }
                        }
                        if (sumOfSimilarityForTorrent[torr1] != 0 && weightedSum != 0)
                            predictionVector.Add(Tuple.Create(torr1, weightedSum / sumOfSimilarityForTorrent[torr1]));
                    }
                }
            }
            else
            {
                // The IP is in the database, use IP & kickassUrls
                if (mappedID < invertedAssociationTable.Count)
                {
                    for (int torr1 = 0; torr1 < associationTable.Count; torr1++)
                    {
                        if (!mergeIDs.Contains(torr1) && !invertedAssociationTable[mappedID].Contains(torr1))
                        {
                            // Dont make predictions for kickassurls and the torrents downloaded by the IP
                            double weightedSum = 0;
                            // Go through all torrents downloaded by mergeID
                            for (int torrIndex = 0; torrIndex < mergeIDs.Count; ++torrIndex)
                            {
                                // Check if there is a similarity for the downloaded torrent and the one we want prediction of
                                int index = itemSimilarityTable[torr1].FindIndex(t => t.Item1 == mergeIDs[torrIndex] || t.Item1 == mappedID);
                                if (index != -1)
                                {
                                    // We found a neighbor torrent with a similarity > 0 and the peer has downloaded it
                                    weightedSum += itemSimilarityTable[torr1][index].Item2;
                                }
                            }
                            if (sumOfSimilarityForTorrent[torr1] != 0 && weightedSum != 0)
                                predictionVector.Add(Tuple.Create(torr1, weightedSum / sumOfSimilarityForTorrent[torr1]));
                        }
                    }
                }
            }

            return predictionVector;
        }
        public List<Tuple<int, double>> GetPrediction(int peerID)
        {
            List<Tuple<int, double>> predictionVector = new List<Tuple<int, double>>();
            // Calculate a rating prediction for all possible torrents
            for (int torr1 = 0; torr1 < associationTable.Count; torr1++)
            {
                // 1. Find all similar torrents
                double weightedSum = 0;
                double sumSimilarity = 0;
                if (itemSimilarityTable[torr1].Count != 0)
                {
                    for (int indexTorr2 = 0; indexTorr2 < itemSimilarityTable[torr1].Count; ++indexTorr2)
                    {
                        if (associationTable[itemSimilarityTable[torr1][indexTorr2].Item1].Contains(peerID))
                            weightedSum += itemSimilarityTable[torr1][indexTorr2].Item2;
                        sumSimilarity += Math.Abs(itemSimilarityTable[torr1][indexTorr2].Item2);
                    }
                    if (sumSimilarity != 0 && weightedSum != 0)
                        predictionVector.Add(Tuple.Create(torr1, weightedSum / sumSimilarity));
                }
            }
            return predictionVector;
        }
        /// <summary>
        /// As of 22.3.15, MAE of first 17831 assuming >0.5 && != 1 -> recommendend
        /// 0.00185 vs 0.00171 for no recommendations
        /// </summary>
        /// <returns></returns>
        public double CalculateMeanAbsoluteError()
        {
            int highestPeerID = mySQL.GetHighestPeerID();
            double result = 0;
            double resultZeroPredictions = 0;
            int nofPredictions = 0;
            for (int i = 0; i < Program.MAX_PEERS_FOR_MAE && i < highestPeerID; ++i)
            {
                predictionsFast = GetPredictionFast(i);
                nofPredictions += predictionsFast.Count;
                foreach (Tuple<int, double> t in predictionsFast)
                {
                    if (invertedAssociationTable[i].Contains(t.Item1))
                    {
                        // The peer downloaded the torrent we made a prediction for, thus the prediction should be 1
                        if (t.Item2 <= 0.5)
                            result++; // We made a wrong prediction
                        resultZeroPredictions++;
                    }
                    else
                    {
                        // The peer did not download torrent -> prediction should be 0
                        // TODO: Why are there many prediction 1? 
                        if (t.Item2 > 0.5 && t.Item2 != 1)
                            result++;
                    }
                }
                Console.Clear();
                Console.WriteLine("Current MAE (" + i.ToString() + "/" + highestPeerID.ToString() + " peers used): " + (result / (double)nofPredictions).ToString());
                Console.WriteLine("Assuming all predictions are 0 (" + i.ToString() + "/" + highestPeerID.ToString() + " peers used): " + (resultZeroPredictions / (double)nofPredictions).ToString());
            }
            return result;
        }
        /// <summary>
        /// Calculates how items ranked by the user (rated 1) are sorted in the list of all items compared to a random order
        /// </summary>
        /// <returns></returns>
        public double CalculateRankEvaluation()
        {
            Random rand = new Random();
            double sum = 0.0;
            double nofRatings= 0.0;
            int highestPeerID = 0;
            if (Program.IP_BASED)
                highestPeerID = mySQL.GetHighestPeerIDNewIndex();
            else
                highestPeerID = mySQL.GetHighestPeerID();

            for (int i = 0;  i <= highestPeerID; ++i)
            {
                int peerToUse = rand.Next(0, highestPeerID);
                predictionsFast = GetPredictionFast(peerToUse);
                predictionsFast.Sort((a, b) => b.Item2.CompareTo(a.Item2));

                int nofTorrentsRatedAndPredicted = 0;
                for(int c = 0; c< predictionsFast.Count; ++c)
                {
                    Tuple<int, double> t = predictionsFast[c];
                    if (invertedAssociationTable[peerToUse].Contains(t.Item1)) // check if user rated this item
                    {
                        nofTorrentsRatedAndPredicted++;
                        sum += c+1; // position in list starting with 1
                    }
                }
                int rest = invertedAssociationTable[peerToUse].Count - nofTorrentsRatedAndPredicted;
                sum += ((associationTable.Count + predictionsFast.Count) / 2.0) * rest;

                nofRatings += invertedAssociationTable[peerToUse].Count;
                Console.Clear();
                Console.WriteLine("Current evaluation (" + i.ToString() + "/" + highestPeerID.ToString() + " peers used): " +
                    (sum / ((double)nofRatings*(double)associationTable.Count)).ToString());
            }
            return (sum / (nofRatings*(double)associationTable.Count));
        }
        public void ShowFirstPredictions()
        {
            int peerID = 0;
            bool foundUsefulPrediction = false;
            while (foundUsefulPrediction == false)
            {
                //predictions = GetPrediction(peerID);
                predictionsFast = GetPredictionFast(peerID);
                //predictions.Sort((a, b) => b.Item2.CompareTo(a.Item2));
                predictionsFast.Sort((a, b) => b.Item2.CompareTo(a.Item2));
                if (predictionsFast.Count != 0)
                {
                    Console.WriteLine("\nPredictions for PeerID " + peerID.ToString());

                    foreach (Tuple<int, double> t in predictionsFast)
                    {
                        if (t.Item2 >= 0.01)
                        {
                            foundUsefulPrediction = true;
                            Console.WriteLine("TorrentID " + t.Item1.ToString() + " prediction: " + t.Item2.ToString());
                            foreach (string t2 in mySQL.GetTorrentNamesFromMergeID(t.Item1))
                                Console.WriteLine(t2);
                        }
                    }
                }
                peerID++;
            }
        }
        public Tuple<string,string,string,string> GetTorrentDataFromMergeID(int mergeID)
        {
            return mySQL.GetDataFromMergeID(mergeID);
        }
    }
}
