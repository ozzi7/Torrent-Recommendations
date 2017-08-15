using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.Net;
using System.Collections;

namespace TorrentRecommendation
{
    class MySQL
    {
        private MySqlConnection connection;
        private MySqlCommand command;
        private MySqlDataReader reader;
        private List<List<int>> associationTable;
        private List<List<int>> invertedAssociationTable;
        private int maxPeerIDinMergeAssociated;
        private int highestPeerIndexNewIDs;
        // IP based
        private List<IPAddress> peerList = new List<IPAddress>();

        public MySQL()
        {
            string sConnection = "SERVER=localhost;" + "DATABASE=bachelor;" + "UID=root;" + "PASSWORD=xSXLQHfSj9hUsmj4;";

            connection = new MySqlConnection(sConnection);
            command = connection.CreateCommand();
        }
        /// <summary>
        /// Creates a list of lists with all nonzero entries of torrent->peer associations
        /// Update: Similar torrents are merged, f.ex. different languages/formats of same movies.
        /// </summary>
        /// <returns>Success</returns>
        public List<List<int>> GetAssociationTable()
        {
            if (!Program.IP_BASED)
            {
                connection.Open();
                command.CommandText = "SELECT MAX(mergeId) FROM `mergeassociated`;";
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                reader.Read();
                int maxTorrentID = reader.GetInt32("MAX(mergeId)");
                associationTable = new List<List<int>>(maxTorrentID + 1);
                for (int i = 0; i < maxTorrentID + 1; ++i)
                {
                    List<int> list = new List<int>();
                    associationTable.Add(list);
                }
                connection.Close();

                // 2. Get maximum peer ID
                connection.Open();
                command.CommandText = "SELECT MAX(peerId) FROM `mergeassociated`;";
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                reader.Read();
                int maxPeerID = reader.GetInt32("MAX(peerId)");
                maxPeerIDinMergeAssociated = maxPeerID;
                connection.Close();


                command.CommandText = "SELECT  `mergeId`,`peerId` FROM  `mergeassociated`;";
                connection.Open();
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                while (reader.Read())
                {
                    associationTable[reader.GetInt32("mergeId")].Add(reader.GetInt32("peerId"));
                }
                reader.Close();
                connection.Close();

                for (int i = 0; i < maxTorrentID + 1; ++i)
                {
                    associationTable[i].Sort();
                }
                return associationTable;
            }
            else
            {
                // 1. Get maximum mergeID for mapping list
                connection.Open();
                command.CommandText = "SELECT MAX(mergeId) FROM `mergeassociated`;";
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                reader.Read();
                int maxTorrentID = reader.GetInt32("MAX(mergeId)");
                connection.Close();

                // 2. Get maximum peer ID
                connection.Open();
                command.CommandText = "SELECT MAX(peerId) FROM `mergeassociated`;";
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                reader.Read();
                int maxPeerID = reader.GetInt32("MAX(peerId)");
                maxPeerIDinMergeAssociated = maxPeerID;
                connection.Close();

                // peerHashset used for quick lookup if ip already in list peerlist
                // mapping used for old peerid -> newpeerID mapping
                Program.mapping = new List<int>(new int[maxPeerID+1]);

                // 3. Read all peers as C# IPEndPoint
                Hashtable peerHashTable = new Hashtable();

                // Fills peerList with all unique IP peers (implicitly id = place in list)
                connection.Open();
                command.CommandText = "SELECT * FROM `peers` WHERE id <= " + maxPeerID +";";
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                reader.Read();
                
                while(reader.Read())
                {
                    string[] ep = reader.GetString("ipAddress").Split(':');
                    IPAddress ip;
                    IPAddress.TryParse(ep[0], out ip);

                    if (!peerHashTable.ContainsKey(ip.ToString()))
                    {
                        peerList.Add(ip);
                        peerHashTable.Add(ip.ToString(),peerList.Count-1);
                        Program.mapping[reader.GetInt32("id")] = peerList.Count - 1;
                    }
                    else
                    {
                        Program.mapping[reader.GetInt32("id")] = (int)peerHashTable[ip.ToString()];
                    }
                }
                highestPeerIndexNewIDs = peerList.Count - 1;
                connection.Close();

                // Fill associationTable, use mapping to get new id
                connection.Open();
                command.CommandText = "SELECT * FROM `mergeassociated`;";
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                reader.Read();

                associationTable = new List<List<int>>(maxTorrentID + 1);
                for (int i = 0; i < maxTorrentID + 1; ++i)
                {
                    List<int> list = new List<int>();
                    associationTable.Add(list);
                }
                connection.Close();

                connection.Open();
                command.CommandText = "SELECT  `mergeId`,`peerId` FROM  `mergeassociated`;";               
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                while (reader.Read())
                {
                    associationTable[reader.GetInt32("mergeId")].Add(Program.mapping[reader.GetInt32("peerId")]);
                }
                reader.Close();
                connection.Close();

                for (int i = 0; i < maxTorrentID + 1; ++i)
                {
                    associationTable[i].Sort();
                }
                return associationTable;
            }
        }
        public List<List<int>> GetInvertedAssociationTable()
        {
            if (!Program.IP_BASED)
            {
                connection.Open();
                command.CommandText = "SELECT MAX(peerId) FROM `mergeassociated`;";
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                reader.Read();
                int maxPeerID = reader.GetInt32("MAX(peerId)");
                invertedAssociationTable = new List<List<int>>(maxPeerID + 1);
                for (int i = 0; i < maxPeerID + 1; ++i)
                {
                    List<int> list = new List<int>();
                    invertedAssociationTable.Add(list);
                }
                connection.Close();
                command.CommandText = "SELECT  `peerId`,`mergeId` FROM  `mergeassociated`;";
                connection.Open();
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                while (reader.Read())
                {
                    invertedAssociationTable[reader.GetInt32("peerId")].Add(reader.GetInt32("mergeId"));
                }
                reader.Close();
                connection.Close();

                for (int i = 0; i < maxPeerID + 1; ++i)
                {
                    invertedAssociationTable[i].Sort();
                }
            }
            else
            {
                // Easier, could be used for both ip and not ip based, invert already calculated table
                int maxPeerID = peerList.Count;
                invertedAssociationTable = new List<List<int>>(maxPeerID + 1);
                for (int i = 0; i < maxPeerID + 1; ++i)
                {
                    List<int> list = new List<int>();
                    invertedAssociationTable.Add(list);
                }
                for(int i = 0; i< associationTable.Count; ++i)
                {
                    for(int j = 0; j < associationTable[i].Count; ++j)
                    {
                        invertedAssociationTable[associationTable[i][j]].Add(i);
                    }
                }
            }
            return invertedAssociationTable;
        }
        /// <summary>
        /// Find all torrent names associated with some mergeID
        /// </summary>
        /// <param name="mergeID"></param>
        /// <returns></returns>
        public List<string> GetTorrentNamesFromMergeID(int mergeID)
        {
            List<string> resultSet = new List<string>();

            connection.Open();
            command.CommandText = "SELECT torrentId FROM `mergetorrents` WHERE mergeId = " + mergeID.ToString() + ";";
            reader = command.ExecuteReader();
            if (!reader.HasRows) return null;            
            
            List<int> ids = new List<int>();
            while (reader.Read())
            {
                ids.Add(reader.GetInt32("torrentId"));
            }

            reader.Close();
            for (int i = 0; i < ids.Count; ++i)
            {
                command.CommandText = "SELECT torrentname FROM `torrents` WHERE id = " + ids[i].ToString() + ";";
                reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    reader.Read();
                    resultSet.Add(reader.GetString("torrentname"));
                }
                reader.Close();
            }
            connection.Close();
            return resultSet;
        }
        public int GetHighestPeerID()
        {
            connection.Open();
            command.CommandText = "SELECT MAX(id) FROM `peers`;";
            reader = command.ExecuteReader();
            if (!reader.HasRows) { connection.Close(); return 0; }
            reader.Read();
            int maxPeerId = reader.GetInt32("MAX(id)");
            connection.Close();
            if (maxPeerId <= maxPeerIDinMergeAssociated)
                return maxPeerId;
            else
                return maxPeerIDinMergeAssociated;
        }
        public List<int> GetMergeIDFromKickassURL(List<string> aKickassUrls)
        {
            List<int> result = new List<int>();
            List<int> torrentIDs = new List<int>();
            connection.Open();

            if(aKickassUrls.Count != 0)
            {
                command.CommandText = "SELECT id FROM `torrents` WHERE weblink = '" + aKickassUrls[0] + "' ";
                for(int i = 1; i < aKickassUrls.Count; ++i)
                {
                    command.CommandText += "OR weblink = '" + aKickassUrls[i] + "' ";
                }
                command.CommandText += ";";
                reader = command.ExecuteReader();
                if (!reader.HasRows) return null;
                while (reader.Read())
                {
                    torrentIDs.Add(reader.GetInt32("id"));
                }
                reader.Close();

                if(torrentIDs.Count != 0)
                {
                    command.CommandText = "SELECT mergeId FROM `mergetorrents` WHERE torrentId = " + torrentIDs[0] + " ";
                    for(int i = 1; i < torrentIDs.Count; ++i)
                    {
                        command.CommandText += "OR torrentId = " + torrentIDs[i] + " ";
                    }
                    command.CommandText += ";";
                    
                    reader = command.ExecuteReader();
                    if (!reader.HasRows) return null;
                    while (reader.Read())
                    {
                        result.Add(reader.GetInt32("mergeId"));
                    }
                    reader.Close();
                }
            }
            connection.Close();
            return result;
        }
        /// <summary>
        /// Finds the id of the IP+anyport in peers table, then looks up mapping to the new ID used
        /// for IP based recommendations
        /// </summary>
        /// <param name="ip"></param>
        /// <returns></returns>
        public int GetMappedIDFromIP(string ip)
        {
            connection.Open();
            int result = -1;
            command.CommandText = "SELECT id FROM `peers` WHERE ipAddress like '" + ip + ":%' LIMIT 1;";
            reader = command.ExecuteReader();

            if (reader.HasRows)
            {
                reader.Read();
                result = Program.mapping[reader.GetInt32("id")];
            }
            reader.Close();
            connection.Close();
            return result;
        }
        public Tuple<string,string,string,string> GetDataFromMergeID(int mergeID)
        {
            connection.Open();
            Tuple<string, string, string, string> resultTuple = new Tuple<string,string,string,string>("null","null","null","null");
            command.CommandText = "SELECT * FROM torrents where id in (select torrentId from mergetorrents where mergeId = " + mergeID + ") order by seeders+leechers desc LIMIT 1;";
            reader = command.ExecuteReader();

            if (reader.HasRows)
            {
                reader.Read();
                resultTuple = Tuple.Create(reader.GetString("torrentname"), reader.GetString("weblink"), reader.GetString("kickAssAffiliation"), reader.GetInt32("IMDbId").ToString());
            }
            reader.Close();
            connection.Close();
            return resultTuple;
        }
        public int GetHighestPeerIDNewIndex()
        {
            return highestPeerIndexNewIDs;
        }
    }
}
