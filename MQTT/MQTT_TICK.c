/*
 * MQTT_TICK.c
 *
 *  Created on: 05.10.2016
 *      Author: georgi.angelov
 */

#include "SYS.h"
#include "APP.h"
#include "MQTT.h"

#ifdef USE_MQTT

static u32 TICK_timerId;

#define MQTT_NEXT_TICK() SYS_StartTimer( TICK_timerId, MQTT_TICK_INTERVAL/*50 mSec*/, FALSE/*oneShot*/ ) /* PULSE ONE TICK */

static bool socketMode = TRUE; // BLOCKED MODE

/* GLOBAL FUNCTION - FAST START */
void MQTT_SetState(int state, u32 wait)
{
	MQTT.state = state;
	Ql_OS_SendMessage( TASK_TICK, MSG_TICK_START, wait/*mSec*/, FALSE );
}

void MQTT_onSocConnect(s32 errCode)
{
	if( errCode == SOC_WOULDBLOCK ) // REPEAT TICK
	{
		if( socketMode == FALSE )
			MQTT_SetState( ST_MQTT_WAIT_SOC_CONNECT, MQTT_TICK_INTERVAL ); // START TICK FOR TIMEOUT
		return;
	}
	if( errCode == SOC_SUCCESS || errCode == SOC_ALREADY )
	{
		MQTT.socConnected = TRUE;
		MQTT_SEND_MESSAGE( MSG_MQTT_CONNECT, 0, 0 ); // CONNECTED
	} else {
		MQTT.socConnected = FALSE;
		if( errCode == SOC_BEARER_FAIL )
		{
			Ql_OS_SendMessage( TASK_MAIN, MSG_GPRS_DEACT, 0, 0 ); // DEACTIVATE
		} else {
			MQTT_SEND_MESSAGE( MSG_MQTT_START, 0, 0 ); // RECONNECT
		}
	}
	SYS_StopTimer( TICK_timerId );
	MQTT.state = ST_MQTT_IDLE;
	LG(DBG_DEBUG,"MQTT MQTT_onSocConnect( %d )", errCode);
}

void MQTT_onSocClose(s32 errCode)
{
	MQTT_CloseSocket();
	LG(DBG_INFO, "MQTT MQTT_onSocClose( %d )", errCode );
}

void MQTT_SocClose(void)
{
	if( MQTT.socketId > SOC_ERROR )
	{
		LG(DBG_DEBUG, "MQTT_SocClose()");
		Ql_SOC_Close( MQTT.socketId );
	}
	MQTT.socketId = SOC_ERROR;
}

static void MQTT_onTimer(u32 timerId, void * param) // 50 mSec
{
	s32 res, code;
	u64 acked;
	switch( MQTT.state )
	{
		case ST_MQTT_SOC_CONNECT:
			LG(DBG_INFO, "MQTT Connecting: %s:%d", App.Cfg.ServerAddress, App.Cfg.ServerPort);
			if( SYS_SOC_Create( &MQTT.socketId ) < 0 )
			{
				LG(DBG_ERROR, "SYS_SOC_Create()");
				MQTT_SocClose();
			} else {
				MQTT.timeout = MQTT_CONNECT_TIMEOUT;
				res = Ql_SOC_ConnectEx( MQTT.socketId, (u32)MQTT.ipHex, MQTT.port, socketMode );
				MQTT_onSocConnect( res ); // TEST ERROR
			}
			break;

		case ST_MQTT_WAIT_SOC_CONNECT: // WAIT MQTT_onSocConnect()
			MQTT.timeout -= MQTT_TICK_INTERVAL;
			if( MQTT.timeout > 0 )
			{
				goto STATE_AGAIN; // REPEAT STATE
			} else {
				MQTT_SocClose();
			}
			break;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		case ST_MQTT_SOC_EXECUTE:
			if( !MQTT.socConnected )
				goto STATE_ERROR;
			MQTT.timeout -= MQTT_TICK_INTERVAL;
			if( MQTT.timeout > 0 )
			{
				code = MQTT.soc_func( MQTT.socketId, MQTT.pBuffer + MQTT.totalSize, MQTT.maxSize - MQTT.totalSize );
				if( code == SOC_WOULDBLOCK )
					goto STATE_AGAIN; // WAIT HERE
				if( code < 1 ) // CAN NOT BE ZERO
					goto STATE_ERROR; // BAD SOCKET
				MQTT.totalSize += code;
				if( MQTT.soc_func == Ql_SOC_Send )
				{
					MQTT.sentCounter += code;
					MQTT.state = ST_MQTT_SOC_ACK;
					goto STATE_AGAIN;
				}
STATE_RETURN:
				MQTT.state = ST_MQTT_SOC_EXECUTE;
				if( MQTT.totalSize == MQTT.maxSize ) // IF SUCCESS
					goto STATE_END; // SEND DONE
			} else { // TIMEOUT
STATE_ERROR:
				MQTT.totalSize = 0;
				goto STATE_END;
			}
STATE_AGAIN:
			MQTT_NEXT_TICK(); // REPEAT STATE
			return;
STATE_END:
			Ql_OS_SetEvent( eventId, EVENT_MQTT_EXECUTE ); // UNBLOCK EVENT
			return;

		case ST_MQTT_SOC_ACK:
			MQTT.timeout -= MQTT_TICK_INTERVAL;
			if( MQTT.timeout > 0 )
			{
				res = Ql_SOC_GetAckNumber(MQTT.socketId, &acked);
				if( res < 0)
					goto STATE_ERROR;
				if( acked == MQTT.sentCounter )
					goto STATE_RETURN; // DATA RECEIVED
				goto STATE_AGAIN;
			} else // TIMEOUT
				goto STATE_ERROR;

		default: break; // ST_MQTT_IDLE
	}
}

void proc_srv_tick(s32 TaskId)
{
	ST_MSG taskMsg;
	if( SYS_CreateTimer( &TICK_timerId, MQTT_onTimer, 0, 0 ) )
    	goto BSD;
	LG(DBG_INFO,"BEGIN TICK");
	while( 1 )
	{
		Ql_OS_GetMessage( &taskMsg );
		switch( taskMsg.message )
		{
			case MSG_TICK_START:
				SYS_StartTimer( TICK_timerId, taskMsg.param1, taskMsg.param2 );
				break;
			default:
				break;
		}
	}
BSD: SYS_BSD( "TASK TICK" );
}

#else
void proc_srv_tick(s32 TaskId)
{
	ST_MSG taskMsg;
	LG(DBG_INFO,"BEGIN TICK");
	while( 1 )
	{
		Ql_OS_GetMessage( &taskMsg );
	}
}
#endif // USE_MQTT
