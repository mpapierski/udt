/*****************************************************************************
Copyright � 2001 - 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

UDP-based Data Transfer Library (UDT) version 4

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

UDT is free software; you can redistribute it and/or modify it under the
terms of the GNU Lesser General Public License as published by the Free
Software Foundation; either version 3 of the License, or (at your option)
any later version.

UDT is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for
more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
This header file contains the definition of UDT/CCC base class.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 10/03/2007
*****************************************************************************/


#include "core.h"
#include "ccc.h"
#include <cmath>


CCC::CCC():
m_iSYNInterval(CUDT::m_iSYNInterval),
m_dPktSndPeriod(1.0),
m_dCWndSize(16.0),
m_iACKPeriod(0),
m_iACKInterval(0),
m_bUserDefinedRTO(false),
m_iRTO(-1)
{
}

void CCC::setACKTimer(const int& msINT)
{
   m_iACKPeriod = msINT;

   if (m_iACKPeriod > m_iSYNInterval)
      m_iACKPeriod = m_iSYNInterval;
}

void CCC::setACKInterval(const int& pktINT)
{
   m_iACKInterval = pktINT;
}

void CCC::setRTO(const int& usRTO)
{
   m_bUserDefinedRTO = true;
   m_iRTO = usRTO;
}

void CCC::sendCustomMsg(CPacket& pkt) const
{
   CUDT* u = CUDT::getUDTHandle(m_UDT);

   if (NULL != u)
      u->m_pSndQueue->sendto(u->m_pPeerAddr, pkt);
}

const CPerfMon* CCC::getPerfInfo()
{
   CUDT* u = CUDT::getUDTHandle(m_UDT);
   if (NULL != u)
      u->sample(&m_PerfInfo, false);

   return &m_PerfInfo;
}

void CCC::setMSS(const int& mss)
{
   m_iMSS = mss;
}

void CCC::setBandwidth(const int& bw)
{
   m_iBandwidth = bw;
}

void CCC::setSndCurrSeqNo(const int32_t& seqno)
{
   m_iSndCurrSeqNo = seqno;
}

void CCC::setRcvRate(const int& rcvrate)
{
   m_iRcvRate = rcvrate;
}

void CCC::setMaxCWndSize(const int& cwnd)
{
   m_dMaxCWndSize = cwnd;
}

void CCC::setRTT(const int& rtt)
{
   m_iRTT = rtt;
}

//
void CUDTCC::init()
{
   m_iRCInterval = m_iSYNInterval;
   m_LastRCTime = CTimer::getTime();
   setACKTimer(m_iRCInterval);

   m_bSlowStart = true;
   m_iLastAck = m_iSndCurrSeqNo;
   m_bLoss = false;
   m_iLastDecSeq = CSeqNo::decseq(m_iLastAck);
   m_dLastDecPeriod = 1;
   m_iAvgNAKNum = 0;
   m_iNAKCount = 0;
   m_iDecRandom = 1;

   m_dCWndSize = 16;
   m_dPktSndPeriod = 1;
}

void CUDTCC::onACK(const int32_t& ack)
{
   uint64_t currtime = CTimer::getTime();
   if (currtime - m_LastRCTime < (uint64_t)m_iRCInterval)
      return;

   m_LastRCTime = currtime;

   if (m_bSlowStart)
   {
      m_dCWndSize += CSeqNo::seqlen(m_iLastAck, ack);
      m_iLastAck = ack;

      if (m_dCWndSize > m_dMaxCWndSize)
      {
         m_bSlowStart = false;
         if (m_iRcvRate > 0)
            m_dPktSndPeriod = 100000.0 / m_iRcvRate;
         else
            m_dPktSndPeriod = m_dCWndSize / (m_iRTT + m_iRCInterval);
      }
   }
   else
      m_dCWndSize = m_dCWndSize * 0.875 + m_iRcvRate / 1000000.0 * (m_iRTT + m_iRCInterval) * 0.125;

   // During Slow Start, no rate increase
   if (m_bSlowStart)
      return;

   if (m_bLoss)
   {
      m_bLoss = false;
      return;
   }

   int64_t B = (int64_t)(m_iBandwidth - 1000000.0 / m_dPktSndPeriod);
   if ((m_dPktSndPeriod > m_dLastDecPeriod) && ((m_iBandwidth / 9) < B))
      B = m_iBandwidth / 9;

   double inc;

   if (B <= 0)
      inc = 1.0 / m_iMSS;
   else
   {
      // inc = max(10 ^ ceil(log10( B * MSS * 8 ) * Beta / MSS, 1/MSS)
      // Beta = 1.5 * 10^(-6)

      inc = pow(10.0, ceil(log10(B * m_iMSS * 8.0))) * 0.0000015 / m_iMSS;

      if (inc < 1.0/m_iMSS)
         inc = 1.0/m_iMSS;
   }

   m_dPktSndPeriod = (m_dPktSndPeriod * m_iRCInterval) / (m_dPktSndPeriod * inc + m_iRCInterval);
}

void CUDTCC::onLoss(const int32_t* losslist, const int&)
{
   //Slow Start stopped, if it hasn't yet
   if (m_bSlowStart)
   {
      m_bSlowStart = false;
      if (m_iRcvRate > 0)
         m_dPktSndPeriod = 100000.0 / m_iRcvRate;
      else
         m_dPktSndPeriod = m_dCWndSize / (m_iRTT + m_iRCInterval);
   }

   m_bLoss = true;

   if (CSeqNo::seqcmp(losslist[0] & 0x7FFFFFFF, m_iLastDecSeq) > 0)
   {
      m_dLastDecPeriod = m_dPktSndPeriod;
      m_dPktSndPeriod = ceil(m_dPktSndPeriod * 1.125);

      m_iAvgNAKNum = (int)ceil(m_iAvgNAKNum * 0.875 + m_iNAKCount * 0.125);
      m_iNAKCount = 1;
      m_iDecCount = 1;

      m_iLastDecSeq = m_iSndCurrSeqNo;

      // remove global synchronization using randomization
      srand(m_iLastDecSeq);
      m_iDecRandom = (int)ceil(rand() * double(m_iAvgNAKNum) / (RAND_MAX + 1.0));
      if (m_iDecRandom < 1)
         m_iDecRandom = 1;
   }
   else if ((m_iDecCount ++ < 5) && (0 == (++ m_iNAKCount % m_iDecRandom)))
   {
      // 0.875^5 = 0.51, rate should not be decreased by more than half within a congestion period
      m_dPktSndPeriod = ceil(m_dPktSndPeriod * 1.125);
      m_iLastDecSeq = m_iSndCurrSeqNo;
   }
}

void CUDTCC::onTimeout()
{
   if (m_bSlowStart)
   {
      m_bSlowStart = false;
      if (m_iRcvRate > 0)
         m_dPktSndPeriod = 100000.0 / m_iRcvRate;
      else
         m_dPktSndPeriod = m_dCWndSize / (m_iRTT + m_iRCInterval);
   }
}
