/*
 * Program.cs
 * 
 * A very basic example program that serves as an NChord server running on the command line.
 *
 * ****************************************************************************
 *  Copyright (c) 2008 Andrew Cencini
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * ****************************************************************************
 */

using System;
using System.Collections.Generic;
using System.Text;

using NChordLib;

namespace NChordServer
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                if (args.Length == 1)
                {
                    // start new ring
                    int portNum = Convert.ToInt32(args[0]);
                    ChordServer.LocalNode = new ChordNode(System.Net.Dns.GetHostName(), portNum);

                    if (ChordServer.RegisterService(portNum))
                    {
                        ChordInstance instance = ChordServer.GetInstance(ChordServer.LocalNode);
                        instance.Join(null, ChordServer.LocalNode.Host, ChordServer.LocalNode.PortNumber);
                        while (true)
                        {
                            switch (Char.ToUpperInvariant(Console.ReadKey(true).KeyChar))
                            {
                                case 'I':
                                    {
                                        PrintNodeInfo(instance, false);
                                        break;
                                    }
                                case 'X':
                                    {
                                        PrintNodeInfo(instance, true);
                                        break;
                                    }
                                case '?':
                                    {
                                        Console.WriteLine("Get Server [I]nfo, E[x]tended Info, [Q]uit, or Get Help[?]");
                                        break;
                                    }
                                case 'Q':
                                    {
                                        instance.Depart();
                                        return;
                                    }
                                default:
                                    {
                                        Console.WriteLine("Get Server [I]nfo, E[x]tended Info, [Q]uit, or Get Help[?]");
                                        break;
                                    }
                            }
                        }
                    }
                }
                else if (args.Length == 3)
                {
                    // join to existing node
                    int portNum = Convert.ToInt32(args[0]);
                    int seedPort = Convert.ToInt32(args[2]);
                    ChordServer.LocalNode = new ChordNode(System.Net.Dns.GetHostName(), portNum);

                    if (ChordServer.RegisterService(portNum))
                    {
                        ChordInstance instance = ChordServer.GetInstance(ChordServer.LocalNode);
                        instance.Join(new ChordNode(args[1], seedPort), ChordServer.LocalNode.Host, ChordServer.LocalNode.PortNumber);
                        while (true)
                        {
                            switch (Char.ToUpperInvariant(Console.ReadKey(true).KeyChar))
                            {
                                case 'I':
                                    {
                                        PrintNodeInfo(instance, false);
                                        break;
                                    }
                                case 'X':
                                    {
                                        PrintNodeInfo(instance, true);
                                        break;
                                    }
                                case '?':
                                    {
                                        Console.WriteLine("Get Server [I]nfo, E[x]tended Info, [Q]uit, or Get Help[?]");
                                        break;
                                    }
                                case 'Q':
                                    {
                                        instance.Depart();
                                        return;
                                    }
                                default:
                                    {
                                        Console.WriteLine("Get Server [I]nfo, E[x]tended Info, [Q]uit, or Get Help[?]");
                                        break;
                                    }
                            }
                        }
                    }
                }
                else
                {
                    Usage();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unhandled exception: {0}", ex);
                Usage();
            }
        }

        /// <summary>
        /// Print usage information.
        /// </summary>
        static void Usage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("\tNChord.exe <portToRunOn> [ <seedHost> <seedPort> ]");
        }

        /// <summary>
        /// Print information about a given Chord node.
        /// </summary>
        /// <param name="instance">The Chord instance to get information from.</param>
        /// <param name="extended">Whether or not to print extended information.</param>
        static void PrintNodeInfo(ChordInstance instance, bool extended)
        {
            ChordNode successor = instance.Successor;
            ChordNode predecessor = instance.Predecessor;
            ChordFingerTable fingerTable = instance.FingerTable;
            ChordNode[] successorCache = instance.SuccessorCache;

            string successorString, predecessorString, successorCacheString, fingerTableString;
            if (successor != null)
            {
                successorString = successor.ToString();
            }
            else
            {
                successorString = "NULL";
            }

            if (predecessor != null)
            {
                predecessorString = predecessor.ToString();
            }
            else
            {
                predecessorString = "NULL";
            }

            successorCacheString = "SUCCESSOR CACHE:";
            for (int i = 0; i < successorCache.Length; i++)
            {
                successorCacheString += string.Format("\n\r{0}: ", i);
                if (successorCache[i] != null)
                {
                    successorCacheString += successorCache[i].ToString();
                }
                else
                {
                    successorCacheString += "NULL";
                }
            }

            fingerTableString = "FINGER TABLE:";
            for (int i = 0; i < fingerTable.Length; i++)
            {
                fingerTableString += string.Format("\n\r{0:x8}: ", fingerTable.StartValues[i]);
                if (fingerTable.Successors[i] != null)
                {
                    fingerTableString += fingerTable.Successors[i].ToString();
                }
                else
                {
                    fingerTableString += "NULL";
                }
            }

            Console.WriteLine("\n\rNODE INFORMATION:\n\rSuccessor: {1}\r\nLocal Node: {0}\r\nPredecessor: {2}\r\n", ChordServer.LocalNode, successorString, predecessorString);

            if (extended)
            {
                Console.WriteLine("\n\r" + successorCacheString);

                Console.WriteLine("\n\r" + fingerTableString);
            }
        }
    }
}
