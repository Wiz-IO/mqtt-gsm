/*
 * MQTT_APP.c
 *
 *  Created on: 04.10.2016
 *  Author: Georgi Angelov
 */

#include "APP.h"
#include "MQTT.h"

#ifdef USE_MQTT

MQTTClient_t  MQTT;

char S_TOPIC[MQTT_TOPIC_SIZE];	/* SERVER CHANNEL Client Read - SUBSCRIBE */
char C_TOPIC[MQTT_TOPIC_SIZE];	/* CLIENT CHANNEL Clent Write */
char L_TOPIC[MQTT_TOPIC_SIZE];	/* CLIENT STATUS  Log Write */

static unsigned char MQTT_WriteBuff[MQTT_BUFFER_SIZE];
static unsigned char MQTT_ReadBuff[MQTT_BUFFER_SIZE];
static MQTTPacket_connectData connectData = MQTTPacket_connectData_initializer;

void MQTT_CloseSocket(void)
{
	MQTT_SocClose();
    MQTT.isConnected  		= FALSE;
    MQTT.socConnected 		= FALSE;
    MQTT.sentCounter  		= 0; // CLEAR SOCKET ACK COUNTER
    MQTT.pingOutstanding 	= FALSE;
    Ql_memset(MQTT.Tx, 0, sizeof(MQTT.Tx));
    Ql_memset(MQTT.Rx, 0, sizeof(MQTT.Rx));
    MQTT_heapInit(); // CLEAR ALL MESSAGES
    LED_SET(0);
}

static void MQTT_Reset(void)
{
	MQTT_CloseSocket();
	Ql_OS_SendMessage(TASK_SRV, MSG_MQTT_START, 0, 0);
	LG(DBG_ERROR, "MQTT Reseting..." );
}

static int MQTT_Init(void)
{
    Ql_sprintf( S_TOPIC, "S/%s", App.IMEI ); // SERVER CHANNEL
    Ql_sprintf( C_TOPIC, "C/%s", App.IMEI ); // CLIENT CHANNEL
    Ql_sprintf( L_TOPIC, "L/%s", App.IMEI ); // LOGGER CHANNEL IMEI is optimal
    Ql_memset( &MQTT, 0, sizeof( MQTTClient_t ) );
	connectData.willFlag 			= 0;
	connectData.MQTTVersion 		= 3;
	connectData.clientID.cstring 	= App.IMEI;
	connectData.username.cstring 	= App.IMEI;
	connectData.password.cstring 	= App.Cfg.ServerPass;
	connectData.keepAliveInterval 	= App.Cfg.ServerTTL; // secunds
	connectData.cleansession 		= 1;
    MQTT.socketId     				= SOC_ERROR;
    MQTT.Tx 		  				= MQTT_WriteBuff;
    MQTT.Rx 		  				= MQTT_ReadBuff;
	MQTT.port 						= App.Cfg.ServerPort;
    MQTT.pingOutstanding 			= FALSE;
    MQTT.keepAliveInterval_ms 		= ( connectData.keepAliveInterval * 1000 ); /* to mSec */
    MQTT.defaultMessageHandler 		= NULL;
    MQTT_CloseSocket(); // JUST CLOSE SOCKET
    Ql_memset( &MQTT.messageHandlers, 0, sizeof( MessageHandlers_t ) * MAX_MESSAGE_HANDLERS );
	s32 res;
    if(( res = Ql_IpHelper_ConvertIpAddr((u8*)App.Cfg.ServerAddress, (u32*)MQTT.ipHex) ))
    {
    	LG(DBG_ERROR, "MQTT Convert IP ( %d )", res );
    	return res;
    }
	return OK;
}

static int checkNetwork(void)
{
	if( App.isNetwork == FALSE )
	{
		//Ql_OS_SendMessage( TASK_MAIN, MSG_GPRS_DEACT, 0, 0 ); // DEACTIVATE
		LG(DBG_ERROR,"MQTT NO NETWORK");
		return FALSE;
	} else {
		return TRUE;
	}
}

void proc_srv_task(s32 TaskId)
{
	ST_MSG taskMsg;
	LG(DBG_INFO,"BEGIN MQTT");
	while( 1 )
	{
		Ql_OS_GetMessage( &taskMsg );
		switch( taskMsg.message )
		{
			case MSG_MQTT_INIT: // FROM MAIN, AFTER APP_INIT
				LG(DBG_INFO,"MSG_MQTT_INIT");
				if( MQTT_Init() )
					goto BSD;
				break;

			case MSG_MQTT_START: // FROM MAIN
				LG(DBG_INFO,"MSG_MQTT_START");
				MQTT_CloseSocket(); // JUST CLOSE
				Ql_OS_SendMessage(TASK_MAIN, MSG_SET_ICON, ICON_NET, 0);
				if( checkNetwork() )
				{
					Ql_OS_SendMessage(TASK_MAIN, MSG_PRINT_TEXT, (u32)"Server Connecting", 0);
					MQTT_SetState( ST_MQTT_SOC_CONNECT, 10 ); // AFTER 10 mSec
				}
				// else wait gprs
				break;

        	case MSG_MQTT_CONNECT: // FROM MQTT_cbConnect()
        		LG(DBG_INFO,"MSG_MQTT_CONNECT");
        		if( checkNetwork() )
        		{
					if( MQTT_Connect( &MQTT, &connectData ) == OK ) ////// BLOCKED //////
					{
						MQTT_SEND_MESSAGE( MSG_MQTT_SUBSCRIBE, 0, 0 );
						LED_SET(1);
					} else {
						LG(DBG_ERROR, "MQTT CONNECT");
						MQTT_Reset();
					}
        		} // else wait gprs
        		break;

        	case MSG_MQTT_SUBSCRIBE:
    			LG(DBG_INFO,"MSG_MQTT_SUBSCRIBE: %s", S_TOPIC);
    			if( checkNetwork() )
    			{
					if( MQTT_Subscribe( &MQTT, S_TOPIC, QOS0, MQTT_onMessage ) == OK ) ////// BLOCKED //////
					{
						MQTT_SEND_MESSAGE( MSG_MQTT_PING, 0, 0 );
						Ql_OS_SendMessage(TASK_MAIN, MSG_SET_ICON, ICON_SRV, 0);
						Ql_OS_SendMessage(TASK_MAIN, MSG_PRINT_TEXT, (u32)"SERVER CONNECTED", 0);
					} else {
						LG(DBG_ERROR, "MQTT SUBSCRIBE");
						MQTT_Reset();
					}
    			} // else wait gprs
    			break;

        	case MSG_MQTT_PING:
        		if( checkNetwork() )
        		{
					if( MQTT_Keepalive( &MQTT ) == OK ) ////// BLOCKED //////
					{
						MQTT_SEND_MESSAGE( MSG_MQTT_PING, 0, 0 ); // PING AGAIN
						break;
					} else {
						MQTT_Reset();
					}
        		} // else wait gprs
        		break;
		}//SWITCH
	}//WHILE
BSD: SYS_BSD( "TASK MQTT" );
}//END PROC
#else
void proc_srv_task(s32 TaskId)
{
	ST_MSG taskMsg;
	LG(DBG_INFO,"MQTT NOT USED");
	while( 1 )
	{
		Ql_OS_GetMessage( &taskMsg );
	}
}
#endif // USE_MQTT



