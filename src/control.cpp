/*****************************************************************************
Copyright © 2001 - 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

UDP-based Data Transfer Library (UDT) version 4

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
This file contains the implementation of UDT congestion control block.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 07/11/2007
*****************************************************************************/

#include <string.h>
#include "control.h"

using namespace std;

CHistory::CHistory():
m_uiSize(1024)
{
   pthread_mutex_init(&m_Lock, NULL);
}

CHistory::CHistory(const unsigned int& size):
m_uiSize(size)
{
}

CHistory::~CHistory()
{
   for (set<CHistoryBlock*, CTSComp>::iterator i = m_sTSIndex.begin(); i != m_sTSIndex.end(); ++ i)
      delete *i;
}

void CHistory::update(const sockaddr* addr, const int& ver, const int& rtt, const int& bw)
{
   CHistoryBlock* hb = new CHistoryBlock;
   convert(addr, ver, hb->m_IP);

   set<CHistoryBlock*, CIPComp>::iterator i = m_sIPIndex.find(hb);

   if (i == m_sIPIndex.end())
   {
      hb->m_iRTT = rtt;
      hb->m_iBandwidth = bw;
      hb->m_ullTimeStamp = CTimer::getTime();
      m_sIPIndex.insert(hb);
      m_sTSIndex.insert(hb);

      if (m_sTSIndex.size() > m_uiSize)
      {
         hb = *m_sTSIndex.begin();
         m_sIPIndex.erase(hb);
         m_sTSIndex.erase(m_sTSIndex.begin());
      }
   }
   else
   {
      delete hb;
      (*i)->m_iRTT = rtt;
      (*i)->m_iBandwidth = bw;
      (*i)->m_ullTimeStamp = CTimer::getTime();
   }
}

int CHistory::lookup(const sockaddr* addr, const int& ver, CHistoryBlock* hb)
{
   convert(addr, ver, hb->m_IP);

   set<CHistoryBlock*, CIPComp>::iterator i = m_sIPIndex.find(hb);

   if (i == m_sIPIndex.end())
      return -1;

   hb->m_ullTimeStamp = (*i)->m_ullTimeStamp;
   hb->m_iRTT = (*i)->m_iRTT;
   hb->m_iBandwidth = (*i)->m_iBandwidth;

   return 1;
}

void CHistory::convert(const sockaddr* addr, const int& ver, uint32_t* ip)
{
   if (ver == AF_INET)
   {
      ip[0] = ((sockaddr_in*)addr)->sin_addr.s_addr;
      ip[1] = ip[2] = ip[3] = 0;
   }
   else
   {
      memcpy((char*)ip, (char*)((sockaddr_in6*)addr)->sin6_addr.s6_addr, 16);
   }
}
