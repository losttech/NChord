/*
 * ChordServer.cs:
 * 
 *  ChordServer is a kitchen sink of static methods and properties for use in:
 * 
 *      * Safely interacting with the Chord DHT (simplifying retry & exception handling) locally and remotely.
 *      * Getting a raw ChordInstance remoting instance (do your own exception handling / validation).
 *      * Performing "Chord-math" for doing wraparound comparisons on IDs and finger table entries.
 *      * Logging to a common logging facility (used by client and server code alike).
 *      * Common remoting service registration / un-regstration.
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
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;

namespace NChordLib
{
    /// <summary>
    /// Static methods and properties for various Chord functionality.
    /// </summary>
    public static partial class ChordServer
    {
        private static ChordNode s_LocalNode = null;
        /// <summary>
        /// The local ChordNode identification.  Used in logging (to log to the correct log file) and navigation.
        /// </summary>
        public static ChordNode LocalNode
        {
            get 
            { 
                return s_LocalNode; 
            }
            set 
            { 
                s_LocalNode = value; 
            }
        }

        /// <summary>
        /// Get a (local or remote) ChordInstance given a ChordNode.
        /// </summary>
        /// <param name="node">The ChordNode specifying the node to get an instance of.</param>
        /// <returns>A ChordInstance from the specified node, or null if an error is encountered.</returns>
        public static ChordInstance GetInstance(ChordNode node)
        {
            if (node == null)
            {
                ChordServer.Log(LogLevel.Error, "Navigation", "Invalid Node ({0}).", "Null Argument.");
                return null;
            }

            try
            {
                ChordInstance retInstance = (ChordInstance)Activator.GetObject(typeof(ChordInstance), string.Format("tcp://{0}:{1}/chord", node.Host, node.PortNumber));
                return retInstance;
            }
            catch (Exception e)
            {
                // perhaps instead we should just pass on the error?
                ChordServer.Log(LogLevel.Error, "Navigation", "Unable to activate remote server {0}:{1} ({2}).", node.Host, node.PortNumber, e.Message);
                return null;
            }
        }

        #region Safe Remote Method / Property Access

        /*
         * Retry logic:
         *  The idea behind the retry logic is to provide a simple and common-case reusable call
         *  in to remote methods or properties.  This logic also serendipitously encapsulates and
         *  simplifies exception handling by performing a bounded number of retries as part of
         *  exception handling.  The retryCount that is passed along as part of the retry logic
         *  serves as a pleasant way to maintain state across node boundaries (thus enforcing a
         *  fixed number of N retries for a logical operation, no matter how many nodes the
         *  operation spans.
         * 
         *  Currently, the default retry count is hardcoded; in the future it may be desirable to
         *  expose this value as a configurable parameter.
         * 
         * Safe access & exception handling pattern:
         *  Anywhere client or server code needs to make remoting calls, there are typically two 
         *  things people usually do: wrap the call in some sort of exception handling (not doing
         *  this is generally silly - and is a quick way to wreck whatever application is consuming
         *  that code upstream), and peform a fixed number of retries in case of transient errors
         *  (transient errors can be somewhat common when testing with many hundreds of Chord nodes
         *  running simultaneously on a single OS instance - often, retrying fatal-seeming errors
         *  can lead to success, reducing the need to exercise (harsher) upstream failure handling).
         *  
         *  In almost all cases, upstream code patterns performing these remote access / invocations
         *  use a single exception handling path; therefore, error is signaled simply via return value
         *  for simple error-handling (since retry is not needed).
         *  
         */


        /// <summary>
        /// Calls Notify() remotely, using a default retry value of three.
        /// </summary>
        /// <param name="remoteNode">The remote on which to call the method.</param>
        /// <param name="callingNode">The node to inform the remoteNode of.</param>
        /// <returns>True if succeeded, FALSE otherwise.</returns>
        public static bool CallNotify(ChordNode remoteNode, ChordNode callingNode)
        {
            return CallNotify(remoteNode, callingNode, 3);
        }

        /// <summary>
        /// Calls Notify() remotely, using a default retry value of three.
        /// </summary>
        /// <param name="remoteNode">The remote on which to call the method.</param>
        /// <param name="callingNode">The node to inform the remoteNode of.</param>
        /// <param name="retryCount">The number of times to retry the operation in case of error.</param>
        /// <returns>True if succeeded, FALSE otherwise.</returns>
        public static bool CallNotify(ChordNode remoteNode, ChordNode callingNode, int retryCount)
        {
            ChordInstance instance = ChordServer.GetInstance(remoteNode);

            try
            {
                instance.Notify(callingNode);
                return true;
            }
            catch (System.Exception ex)
            {
                ChordServer.Log(LogLevel.Debug, "Remote Invoker", "CallNotify error: {0}", ex.Message);

                if (retryCount > 0)
                {
                    return CallNotify(remoteNode, callingNode, --retryCount);
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// Calls FindSuccessor() remotely, using a default retry value of three.  HopCount is ignored.
        /// </summary>
        /// <param name="remoteNode">The remote on which to call the method.</param>
        /// <param name="id">The ID to look up.</param>
        /// <returns>The Successor of ID, or NULL in case of error.</returns>
        public static ChordNode CallFindSuccessor(ChordNode remoteNode, UInt64 id)
        {
            int hopCountOut = 0;
            return CallFindSuccessor(remoteNode, id, 3, 0, out hopCountOut);
        }

        /// <summary>
        /// Convenience function to call FindSuccessor using ChordServer.LocalNode as the
        /// "remote" node.
        /// </summary>
        /// <param name="id"> The ID to look up (ChordServer.LocalNode is used as the remoteNode).</param>
        /// <returns>The Successor of ID, or NULL in case of error.</returns>
        public static ChordNode CallFindSuccessor(UInt64 id)
        {
            return CallFindSuccessor(ChordServer.LocalNode, id);
        }

        /// <summary>
        /// Calls FindSuccessor() remotely, using a default retry value of three.
        /// </summary>
        /// <param name="remoteNode">The remote node on which to call FindSuccessor().</param>
        /// <param name="id">The ID to look up.</param>
        /// <param name="retryCount">The number of times to retry the operation in case of error.</param>
        /// <param name="hopCountIn">The known hopcount prior to calling FindSuccessor on this node.</param>
        /// <param name="hopCountOut">The total hopcount of this operation (either returned upwards, or reported for hopcount efficiency validation).</param>
        /// <returns>The Successor of ID, or NULL in case of error.</returns>
        public static ChordNode CallFindSuccessor(ChordNode remoteNode, UInt64 id, int retryCount, int hopCountIn, out int hopCountOut)
        {
            ChordInstance instance = ChordServer.GetInstance(remoteNode);

            try
            {
                return instance.FindSuccessor(id, hopCountIn, out hopCountOut);
            }
            catch (System.Exception ex)
            {
                ChordServer.Log(LogLevel.Debug, "Remote Invoker", "CallFindSuccessor error: {0}", ex.Message);

                if (retryCount > 0)
                {
                    return CallFindSuccessor(remoteNode, id, --retryCount, hopCountIn, out hopCountOut);
                }
                else
                {
                    hopCountOut = hopCountIn;
                    return null;
                }
            }
        }

        /// <summary>
        /// Convenience function to get the local Successor Cache from ChordServer.LocalNode.
        /// </summary>
        /// <returns>The local node's successorCache, or NULL in case of error.</returns>
        public static ChordNode[] GetSuccessorCache()
        {
            return GetSuccessorCache(ChordServer.LocalNode);
        }

        /// <summary>
        /// Gets the remote SuccessorCache property, using a default retry value of three.
        /// </summary>
        /// <param name="remoteNode">The remote from which to access the Successor Cache.</param>
        /// <returns>The remote node's successorCache, or NULL in case of error.</returns>
        public static ChordNode[] GetSuccessorCache(ChordNode remoteNode)
        {
            return GetSuccessorCache(remoteNode, 3);
        }

        /// <summary>
        /// Gets the remote SuccessorCache property, given a custom retry count.
        /// </summary>
        /// <param name="remoteNode">The remote node from which to access the property.</param>
        /// <param name="retryCount">The number of times to retry the operation in case of error.</param>
        /// <returns>The remote successorCache, or NULL in case of error.</returns>
        public static ChordNode[] GetSuccessorCache(ChordNode remoteNode, int retryCount)
        {
            ChordInstance instance = ChordServer.GetInstance(remoteNode);

            try
            {
                return instance.SuccessorCache;
            }
            catch (System.Exception ex)
            {
                ChordServer.Log(LogLevel.Debug, "Remote Accessor", "GetSuccessorCache error: {0}", ex.Message);

                if (retryCount > 0)
                {
                    return GetSuccessorCache(remoteNode, --retryCount);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Convenience function to get the local node's Predecessor.
        /// </summary>
        /// <returns>The Predecessor of ChordServer.LocalNode, or NULL in case of error.</returns>
        public static ChordNode GetPredecessor()
        {
            return GetPredecessor(ChordServer.LocalNode);
        }

        /// <summary>
        /// Gets the remote Predecessor property, using a default retry value of three.
        /// </summary>
        /// <param name="remoteNode">The remote from which to access the property.</param>
        /// <returns>The remote node's predecessor, or NULL in case of error.</returns>
        public static ChordNode GetPredecessor(ChordNode remoteNode)
        {
            return GetPredecessor(remoteNode, 3);
        }

        /// <summary>
        /// Gets the remote Predecessor property, given a custom retry count.
        /// </summary>
        /// <param name="remoteNode">The remote node from which to access the property.</param>
        /// <param name="retryCount">The number of times to retry the operation in case of error.</param>
        /// <returns>The remote predecessor, or NULL in case of error.</returns>
        public static ChordNode GetPredecessor(ChordNode remoteNode, int retryCount)
        {
            ChordInstance instance = ChordServer.GetInstance(remoteNode);

            try
            {
                return instance.Predecessor;
            }
            catch (System.Exception ex)
            {
                ChordServer.Log(LogLevel.Debug, "Remote Accessor", "GetPredecessor error: {0}", ex.Message);

                if (retryCount > 0)
                {
                    return GetPredecessor(remoteNode, --retryCount);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Convenience function to retrieve the local node's Successor property.
        /// </summary>
        /// <returns>The local node's successor, or NULL in case of error.</returns>
        public static ChordNode GetSuccessor()
        {
            return GetSuccessor(ChordServer.LocalNode);
        }

        /// <summary>
        /// Gets the remote Successor property, using a default retry value of three.
        /// </summary>
        /// <param name="remoteNode">The remote from which to access the property.</param>
        /// <returns>The remote node's successor, or NULL in case of error.</returns>
        public static ChordNode GetSuccessor(ChordNode remoteNode)
        {
            return GetSuccessor(remoteNode, 3);
        }

        /// <summary>
        /// Gets the remote Successor property, given a custom retry count.
        /// </summary>
        /// <param name="remoteNode">The remote node from which to access the property.</param>
        /// <param name="retryCount">The number of times to retry the operation in case of error.</param>
        /// <returns>The remote successor, or NULL in case of error.</returns>
        public static ChordNode GetSuccessor(ChordNode remoteNode, int retryCount)
        {
            ChordInstance instance = ChordServer.GetInstance(remoteNode);

            try
            {
                return instance.Successor;
            }
            catch (System.Exception ex)
            {
                ChordServer.Log(LogLevel.Debug, "Remote Accessor", "GetSuccessor error: {0}", ex.Message);

                if (retryCount > 0)
                {
                    return GetSuccessor(remoteNode, --retryCount);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Safely checks whether a ChordInstance is valid by ensuring the port and successor values are valid.
        /// </summary>
        /// <param name="instance">The ChordInstance to validity-check.</param>
        /// <returns>TRUE if valid; FALSE otherwise.</returns>
        public static bool IsInstanceValid(ChordInstance instance)
        {
            try
            {
                if (instance.Port > 0 && instance.Successor != null)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception e)
            {
                Log(LogLevel.Debug, "Incoming instance was not valid: ({0}).", e.ToString());  // TODO; better logging
                return false;
            }
        }

        #endregion

        #region Chord-math Functionality

        /// <summary>
        /// Checks whether a key is in a specified range.  Handles wraparound for cases where the start value is
        /// bigger than the end value.  Used extensively as a convenience function to determine whether or not a
        /// piece of data belongs in a given location.
        /// 
        /// Most typically, IsIDInRange is used to determine whether a key is between the local ID and the successor ID:
        ///     IsIDInRange(key, this.ID, this.Successor.ID);
        /// </summary>
        /// <param name="id">The ID to range-check.</param>
        /// <param name="start">The "low" end of the range.</param>
        /// <param name="end">The "high" end of the range.</param>
        /// <returns>TRUE if ID is in range; FALSE otherwise.</returns>
        public static bool IsIDInRange(UInt64 id, UInt64 start, UInt64 end)
        {
            if (start >= end)
            {
                // this handles the wraparound and single-node case.  for wraparound, the range includes zero, so any key
                // that is bigger than start or smaller than or equal to end is in the range.  for single-node, our nodehash
                // will equal the successor nodehash (we are our own successor), and there's no way a key can't fall in the range
                // because if range == X, then key must be either >, < or == X which will always happen!
                if (id > start || id <= end)
                {
                    return true;
                }
            }
            else
            {
                // this is the normal case where we want the key to fall between the lower bound of start and the upper bound of end
                if (id > start && id <= end)
                {
                    return true;
                }
            }
            // for all other cases we're not in range
            return false;
        }

        /// <summary>
        /// Range checks to determine if key fits in the range.  In this particular case, if the start==end of the range,
        /// we consider key to be in that range.  Handles wraparound.
        /// </summary>
        /// <param name="key">the key to range check</param>
        /// <param name="start">lower bound of the range</param>
        /// <param name="end">upper bound of the range</param>
        /// <returns>true if in the range; false if key is not in the range</returns>
        public static bool FingerInRange(UInt64 key, UInt64 start, UInt64 end)
        {
            // in this case, we are the successor of the predecessor we're looking for
            // so we return true which will mean return the farthest finger from FindClosestPrecedingFinger
            // ... this way, we can go as far around the circle as we know how to in order to find the
            // predecessor
            if (start == end)
            {
                return true;
            }
            else if (start > end)
            {
                // this handles the wraparound case - since the range includes zero, any key bigger than the start
                // or smaller than the end will be considered in the range
                if (key > start || key < end)
                {
                    return true;
                }
            }
            else
            {
                // this is the normal case - in this case, the start is the lower bound and the end is the upper bound
                // so if key falls between them, we're good
                if (key > start && key < end)
                {
                    return true;
                }
            }
            // for all other cases, we're not in the range
            return false;
        }

        #endregion

        #region Remoting Service Plumbing

        static TcpChannel s_ChordTcpChannel = null;
        /// <summary>
        /// Safely register the TcpChannel for this service.
        /// </summary>
        /// <param name="port">The port on which the service will listen.</param>
        /// <returns>true if registration succeeded; false, otherwise.</returns>
        public static bool RegisterService(int port)
        {
            try
            {
                if (s_ChordTcpChannel != null)
                {
                    ChordServer.UnregisterService();
                }

                BinaryServerFormatterSinkProvider provider = new BinaryServerFormatterSinkProvider();
                provider.TypeFilterLevel = System.Runtime.Serialization.Formatters.TypeFilterLevel.Full;

                IDictionary props = new Hashtable();
                props["port"] = port;

                s_ChordTcpChannel = new TcpChannel(props, null, provider);

                ChannelServices.RegisterChannel(s_ChordTcpChannel, false);
                RemotingConfiguration.RegisterWellKnownServiceType(typeof(ChordInstance), "chord", WellKnownObjectMode.Singleton);
            }
            catch (Exception e)
            {
                ChordServer.Log(LogLevel.Error, "Configuration", "Unable to register Chord Service ({0}).", e.Message);
                return false;
            }

            ChordServer.Log(LogLevel.Info, "Configuration", "Chord Service registered on port {0}.", port);

            return true;
        }

        /// <summary>
        /// Safely unregister the TcpChannel for this service.
        /// </summary>
        public static void UnregisterService()
        {
            if (s_ChordTcpChannel != null)
            {
                ChannelServices.UnregisterChannel(s_ChordTcpChannel);
                s_ChordTcpChannel = null;
            }
        }

        #endregion

        #region Logging

        /// <summary>
        /// Log a message to the Chord logging facility.
        /// </summary>
        /// <param name="logLevel">The log level.</param>
        /// <param name="logArea">The functional source area of the log message.</param>
        /// <param name="message">The message to log.</param>
        public static void Log(LogLevel logLevel, string logArea, string message, params object[] parameters)
        {
            //TODO: implement proper logging
            if (logLevel != LogLevel.Debug)
            {
                Console.WriteLine("{0} {1} > : {2}", DateTime.Now, ChordServer.LocalNode, string.Format(message, parameters));
            }
        }

        #endregion
    }

    /// <summary>
    /// The logging level to use for a given message / log.
    /// </summary>
    public enum LogLevel
    {
        Error,
        Info,
        Warn,
        Debug
    }
}
