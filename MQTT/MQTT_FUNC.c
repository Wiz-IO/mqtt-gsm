/*
 * MQTT_FUNC.c
 *
 *  Created on: 03.09.2016
 *      Author: Georgi Angelov
 */

#include "SYS.h"
#include "MQTT.h"
#include "CMD.h"

#ifdef USE_MQTT

#define MM_MAX_COUNT 	4
static mqtt_mess_t MQTT_Mess[MM_MAX_COUNT];

void MQTT_heapInit(void)
{
	Ql_memset( MQTT_Mess, 0, sizeof(mqtt_mess_t) * MM_MAX_COUNT );
}

void MQTT_pushMessage(mqtt_mess_t * m, char * topic, char * txt)
{
	m->status = TRUE;
	if( txt )
		Ql_strcpy( m->buff, txt );
	m->topic = topic;
	m->message.payload = m->buff;
	m->message.payloadlen = Ql_strlen( m->buff );
	m->message.qos = QOS0;
}

mqtt_mess_t * MQTT_allocMessage(void)
{
	static int ring = 0;
	mqtt_mess_t * m = MQTT_Mess;
	for( int i = 0; i < MM_MAX_COUNT; i++, m++ )
	{
		if( m->status == FALSE )
		{
			ring = 0;
			return m;
		}
	}
	m = &MQTT_Mess[ring];
	ring++;
	ring %= MM_MAX_COUNT;
	return m;
}

static mqtt_mess_t * MQTT_popMessage(void)
{
	mqtt_mess_t * m = MQTT_Mess;
	for( int i = 0; i < MM_MAX_COUNT; i++, m++ )
		if( m->status )
			return m;
	return NULL; // NO MESSAGES
}

static int MQTT_PublishAll(MQTTClient_t * c)
{
	mqtt_mess_t * m;
	while( ( m = MQTT_popMessage() ) )
	{
		if(( m->error = MQTT_Publish( c , m->topic, &m->message ) ))
		{
			LG(DBG_ERROR,"MQTT_Publish( %d )", m->error);
			return m->error;
		}
		m->status = FALSE;
		if( m->cb )
			m->cb( m );
	}
	return OK;
}

///////////////////////////////////////////////////////////////////////////////

static int MQTT_getNextPacketId(MQTTClient_t * c)
{
    return c->next_packetid = ( c->next_packetid == MAX_PACKET_ID ) ? 1 : c->next_packetid + 1;
}

static void MQTT_newMessageData(MessageData * md, MQTTString* aTopicName, MQTTMessage* aMessgage)
{
    md->topicName = aTopicName;
    md->message   = aMessgage;
}

static char MQTT_isTopicMatched(char * topicFilter, MQTTString * topicName)
{
    char* curf = topicFilter;
    char* curn = topicName->lenstring.data;
    char* curn_end = curn + topicName->lenstring.len;
    while (*curf && curn < curn_end)
    {
        if (*curn == '/' && *curf != '/')
            break;
        if (*curf != '+' && *curf != '#' && *curf != *curn)
            break;
        if (*curf == '+')
        {
            char* nextpos = curn + 1;
            while (nextpos < curn_end && *nextpos != '/')
                nextpos = ++curn + 1;
        }
        else if (*curf == '#')
            curn = curn_end - 1;
        curf++;
        curn++;
    };
    return (curn == curn_end) && (*curf == '\0');
}

static int MQTT_deliverMessage(MQTTClient_t * c, MQTTString * topicName, MQTTMessage * message)
{
    int i;
    int rc = ERROR;
    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
    {
        if ( c->messageHandlers[i].topicFilter != 0 &&
        		( MQTTPacket_equals(topicName, (char*)c->messageHandlers[i].topicFilter) ||
        		  MQTT_isTopicMatched((char*)c->messageHandlers[i].topicFilter, topicName) ) )
        {
            if( c->messageHandlers[i].fp != NULL )
            {
                MessageData md;
                MQTT_newMessageData(&md, topicName, message);
                c->messageHandlers[i].fp(&md);
                rc = OK;
            }
        }
    }
    if( rc == ERROR && c->defaultMessageHandler != NULL )
    {
        MessageData md;
        MQTT_newMessageData(&md, topicName, message);
        c->defaultMessageHandler( &md );
        rc = OK;
    }
    return rc;
}

static s32 MQTT_sendPacket(MQTTClient_t * c, s32 length, Timer_t * tmr)
{
    if( !c->socConnected || length < 1 )
        return ERROR;
    s32 sent = MQTT_SocketExecute( Ql_SOC_Send, c->Tx, length, TMR_Left_ms( tmr ) );
    if( sent == length )
    {
    	LH(DBG_DEBUG, "MQTT TRANSMIT: ", (char*)c->Tx, c->totalSize );
    	TMR_Init_ms( &c->pingTimer, c->keepAliveInterval_ms ); // RESET KeepAlive timer
    	return OK;
    } else {
    	LG(DBG_ERROR, "MQTT_sendPacket() SIZE: %d", sent);
    	return ERROR;
    }
}

static int MQTT_readLength(MQTTClient_t * c, int * remainingLen, Timer_t * tmr)
{
    u8 Byte;
    s32 multiplier = 1, counter = 0, rc = ERROR;
    *remainingLen = 0;
    do
    {
        if( ++counter > MAX_NO_OF_REMAINING_LENGTH_BYTES )
        	return ERROR;
        Byte = 0;
        if( ( rc = MQTT_SocketExecute( Ql_SOC_Recv, &Byte, 1, TMR_Left_ms( tmr ) ) ) != 1 )
        	return ERROR;
        *remainingLen += ( ( Byte & 127 ) * multiplier );
        multiplier *= 128;
    } while( ( Byte++ & 128 ) != 0 );
    return counter;
}

static s32 MQTT_readPacket(MQTTClient_t * c, Timer_t * tmr)
{
    MQTTHeader header = { 0 };
    int len, remainingLen = 0, rc = ERROR;
	Ql_memset( c->Rx, 0, MQTT_BUFFER_SIZE );

    /* 1. read the header byte.  This has the packet type in it */
    if( ( len = MQTT_SocketExecute( Ql_SOC_Recv, c->Rx, 1, TMR_Left_ms( tmr ) ) ) != 1 )
    {
        return ERROR;
    }
    /* 2. read the remaining length.  This is variable in itself */
    if( MQTT_readLength(c, &remainingLen, tmr) < 0 )
    {
    	return ERROR;
    }
    len += MQTTPacket_encode( c->Rx + 1, remainingLen ); /* put the original remaining length back into the buffer */

    /* 3. read the rest of the buffer using a callback to supply the rest of the data */
    if( remainingLen > 0 && ( MQTT_SocketExecute( Ql_SOC_Recv, c->Rx + len, remainingLen, TMR_Left_ms( tmr ) ) != remainingLen ) )
    {
    	return ERROR;
    }
    header.byte = c->Rx[0];
    rc = header.bits.type;
	if( c->Rx[0] )
	{
		LH(DBG_DEBUG, "MQTT RECEIVE : ", (char*)c->Rx, remainingLen + 2 );
	}
    return rc;
}

static s32 MQTT_Cycle(MQTTClient_t * c, Timer_t * timer)
{
	s32 packet_type = MQTT_readPacket(c, timer);
	if( packet_type < 1 )
		return ERROR;
	u8 dup, type;
    s32 len = 0, rc = OK;
    u16 mypacketid;
    MQTTString topicName;
    MQTTMessage msg;
    switch( packet_type )
    {
        case PUBLISH:
            if( ( len = MQTTDeserialize_publish((unsigned char*)&msg.dup, (int*)&msg.qos, (unsigned char*)&msg.retained, (unsigned short*)&msg.id,
            		&topicName, (unsigned char**)&msg.payload, (int*)&msg.payloadlen, c->Rx, MQTT_BUFFER_SIZE ) ) != 1 )
            {
            	LG(DBG_ERROR, "MQTTDeserialize_publish( %d )", len );
                goto exit;
            }
            MQTT_deliverMessage( c, &topicName, &msg );
            if( msg.qos != QOS0 )
            {
                if( msg.qos == QOS1 )
                    len = MQTTSerialize_ack( c->Tx, MQTT_BUFFER_SIZE, PUBACK, 0, msg.id );
                else if( msg.qos == QOS2 )
                    len = MQTTSerialize_ack( c->Tx, MQTT_BUFFER_SIZE, PUBREC, 0, msg.id );
                if( len <= 0 )
                {
                	LG(DBG_ERROR, "PUB MQTTSerialize_ack( %d )", len);
                    rc = ERROR;
                } else if( ( rc = MQTT_sendPacket( c, len, timer ) ) != OK )
                   {
                       LG(DBG_ERROR, "ACK MQTT_sendPacket( %d )", rc);
                       rc = ERROR;
                   }
            }
            break;

        case PUBREC:
            if( ( len = MQTTDeserialize_ack( &type, &dup, &mypacketid, c->Rx, MQTT_BUFFER_SIZE ) ) != 1 )
            {
            	LG(DBG_ERROR, "PUBREC MQTTDeserialize_ack( %d )", len);
                rc = ERROR;
            }
            else if( ( len = MQTTSerialize_ack( c->Tx, MQTT_BUFFER_SIZE, PUBREL, 0, mypacketid ) ) <= 0)
            {
            	LG(DBG_ERROR, "PUBREC MQTTSerialize_ack( %d )", len);
                rc = ERROR;
            }
            else if( ( rc = MQTT_sendPacket( c, len, timer ) ) != OK )
            {
            	LG(DBG_ERROR, "PUBREC MQTT_sendPacket( %d )", rc);
                rc = ERROR;
            }
            break;

        case PINGRESP:
            c->pingOutstanding = FALSE;
            LG(DBG_INFO,"PINGRESP");
            break;

        default:
        	break;
    }
exit:
    if( rc == OK )
        rc = packet_type;
    return rc;
}

static s32 MQTT_waitFor(MQTTClient_t * c, s32 packetType, Timer_t * timer)
{
    s32 rc = ERROR;
    do
    {
        if( TMR_Expired( timer ) )
            break;
    } while( (rc = MQTT_Cycle(c, timer)) != packetType );
    return rc;
}

s32 MQTT_Connect(MQTTClient_t * c, MQTTPacket_connectData * options)
{
    if( c->isConnected )
        return ERROR;
    if( !c->socConnected )
        return ERROR;
    s32 rc = ERROR, len;
    MQTTPacket_connectData default_options = MQTTPacket_connectData_initializer;
    if( options == NULL )
        options = &default_options;
    Timer_t connectTimer;
    if( ( len = MQTTSerialize_connect( c->Tx, MQTT_BUFFER_SIZE, options ) ) <= 0 )
    {
    	LG(DBG_ERROR, "CONNECT MQTTSerialize_connect( %d )", len );
        goto exit;
    }
    TMR_Init_ms( &c->pingTimer, c->keepAliveInterval_ms ); // FIRST INIT, NEXT FROM SOC SEND
    TMR_Init_ms( &connectTimer, MQTT_COMMAND_TIMEOUT_MS );
    if( ( rc = MQTT_sendPacket( c, len, &connectTimer ) ) != OK )
    {
    	LG(DBG_ERROR, "CONNECT MQTT_sendPacket( %d )", rc );
        goto exit;
    }
    TMR_Init_ms( &connectTimer, MQTT_COMMAND_TIMEOUT_MS );
    if( MQTT_waitFor( c, CONNACK, &connectTimer ) == CONNACK )
    {
        u8 connack_rc = 255;
        s8 sessionPresent = 0;
        if( ( len = MQTTDeserialize_connack( (unsigned char*)&sessionPresent, &connack_rc, c->Rx, MQTT_BUFFER_SIZE ) ) == 1 )
            rc = connack_rc;
        else {
        	LG(DBG_ERROR, "CONNACK MQTTDeserialize_connack( %d )", len);
            rc = ERROR;
        }
    } else {
    	LG(DBG_ERROR, "CONNACK Wait");
        rc = ERROR;
    }
exit:
    if( rc == OK )
        c->isConnected = TRUE;
    return rc;
}

s32 MQTT_Subscribe(MQTTClient_t * c, char * topicFilter, s32 qos, messageHandler messageHandler)
{
    if( !c->isConnected )
        return ERROR;
    if( !c->socConnected )
        return ERROR;
	s32 rc = ERROR, len;
    Timer_t subscribeTimer;
    MQTTString topic = MQTTString_initializer;
    topic.cstring = (char *)topicFilter;
    if( ( len = MQTTSerialize_subscribe( c->Tx, MQTT_BUFFER_SIZE, 0, MQTT_getNextPacketId( c ), 1, &topic, (int*)&qos ) ) <= 0 )
    {
    	LG(DBG_ERROR, "SUBSCRIBE MQTTSerialize_subscribe( %d )", len);
        goto exit;
    }
    TMR_Init_ms( &subscribeTimer, MQTT_COMMAND_TIMEOUT_MS );
    if( ( rc = MQTT_sendPacket( c, len, &subscribeTimer ) ) != OK ) // send the subscribe packet
    {
    	LG(DBG_ERROR, "SUBSCRIBE MQTT_sendPacket( %d )", rc);
        goto exit; // there was a problem
    }
    if( MQTT_waitFor(c, SUBACK, &subscribeTimer) == SUBACK ) // wait for suback
    {
    	s32 count = 0, grantedQoS = -1;
        unsigned short mypacketid;
        if( MQTTDeserialize_suback( &mypacketid, 1, &count, &grantedQoS, c->Rx, MQTT_BUFFER_SIZE ) == 1 )
            rc = grantedQoS; // 0, 1, 2 or 0x80
        if( rc != 0x80 )
        {
        	s32 i;
            for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
            {
                if (c->messageHandlers[i].topicFilter == 0)
                {
                    c->messageHandlers[i].topicFilter = topicFilter;
                    c->messageHandlers[i].fp = messageHandler;
                    rc = 0;
                    break;
                }
            }
        }
    } else
        rc = ERROR;
exit:
    return rc;
}

s32 MQTT_Publish(MQTTClient_t * c, char * topicName, MQTTMessage * message)
{
    if( !c->isConnected )
        return ERROR;
    if( !c->socConnected )
        return ERROR;
    s32 rc = ERROR, len;
    u16 mypacketid;
    u8 dup, type;
    Timer_t timer;
    MQTTString topic = MQTTString_initializer;
    topic.cstring = (char *)topicName;
    if( message->qos == 1 || message->qos == 2 )
        message->id = MQTT_getNextPacketId(c);
    if( ( len = MQTTSerialize_publish( c->Tx, MQTT_BUFFER_SIZE, 0, message->qos, message->retained, message->id, topic, (unsigned char*)message->payload, message->payloadlen ) ) <= 0 )
    {
    	LG(DBG_ERROR, "PUBLISH MQTTSerialize_publish( %d )", len );
    	goto exit;
    }
    TMR_Init_ms( &timer, MQTT_COMMAND_TIMEOUT_MS );
    if( ( rc = MQTT_sendPacket( c, len, &timer ) ) != OK ) // send the subscribe packet
    {
    	LG(DBG_ERROR, "PUBLISH MQTT_sendPacket( %d, %d )", rc, len );
        goto exit;
    }
    if( message->qos == QOS1 )
    {
        if( MQTT_waitFor( c, PUBACK, &timer ) == PUBACK )
        {
            if( ( len = MQTTDeserialize_ack( &type, &dup, &mypacketid, c->Rx, MQTT_BUFFER_SIZE ) ) != 1 )
            {
            	LG(DBG_ERROR, "PUBACK MQTTDeserialize_ack( %d )", len );
                rc = ERROR;
            }
        } else {
        	LG(DBG_ERROR, "PUBACK Wait" );
            rc = ERROR;
        }
    } else if( message->qos == QOS2 )
    {
        if( MQTT_waitFor(c, PUBCOMP, &timer) == PUBCOMP )
        {
            if( ( len = MQTTDeserialize_ack( &type, &dup, &mypacketid, c->Rx, MQTT_BUFFER_SIZE ) ) != 1 )
            {
            	LG(DBG_ERROR, "PUBCOMP MQTTDeserialize_ack( %d )", len);
                rc = ERROR;
            }
        } else {
        	LG(DBG_ERROR, "PUBCOMP Wait");
            rc = ERROR;
        }
    }
exit:
    return rc;
}

s32 MQTT_Keepalive(MQTTClient_t * c)
{
    if( !c->isConnected )
        return ERROR;
    if( !c->socConnected )
        return ERROR;

    s32 rc = MQTT_PublishAll( c );
    if( rc )
    	return rc;

    int len;
    Timer_t timer;
    TMR_Init_ms( &timer, MQTT_CYCLE_TIMEOUT_MS );
    MQTT_Cycle( c, &timer );

    if( c->keepAliveInterval_ms == 0 ) // IF DISABLED
    	return OK;

    if( TMR_Expired( &c->pingTimer ) )
    {
        if( c->pingOutstanding == FALSE )
        {
        	LG(DBG_INFO,"PING\n");
            if( ( len = MQTTSerialize_pingreq( c->Tx, MQTT_BUFFER_SIZE ) ) > 0 )
            {
            	Timer_t timer;
            	TMR_Init_ms( &timer, MQTT_COMMAND_TIMEOUT_MS );
				if( ( rc = MQTT_sendPacket( c, len, &timer ) ) == OK )
				{
					c->pingOutstanding = TRUE;
        			Ql_OS_SendMessage(TASK_MAIN, MSG_SET_ICON, ICON_SRV, 0); // KICK DEVICE WATCHDOG
				}
            }
        }
    }
    return rc;
}

/////////////////////////////////////////////////////////////////////////////////////////////

int MQTT_onMessage(MessageData * rxData)
{
	if(rxData->message->payloadlen > MQTT_BUFFER_SIZE-1)
	{
		LG(DBG_ERROR, "MQTT_onMessage( %d ) too long", rxData->message->payloadlen );
		return ERROR;
	}
	char * pEnd = (char*)rxData->message->payload; // CLOSE STRING
	pEnd[ rxData->message->payloadlen ] = 0;
//// DEBUG
	char txt[256];
	Ql_sprintf( txt, "MQTT_onMessage[%d]: %.*s", rxData->message->payloadlen, rxData->message->payloadlen, (char*)rxData->message->payload);
	LG(DBG_DEBUG, txt );
////
	mqtt_mess_t txMess;
	CMD_DecodeCommand( rxData->message->payload, txMess.buff );
	txMess.message.payload = txMess.buff;
	txMess.message.payloadlen = Ql_strlen( txMess.buff );
	txMess.message.qos = QOS0;
	if( MQTT_Publish( &MQTT , C_TOPIC, &txMess.message ) != OK )
	{
		LG(DBG_ERROR, "PUBLISH: MQTT_onMessage()");
		return ERROR;
	}
	return OK;
}

/////////////////////////////////////////////////////////////////////////////////////////////

int MQTT_SocketExecute(s32(*func)(s32,u8*,s32), u8* dst, int size, long timeout)
{
	LED_SET( 1 );
	MQTT.totalSize = 0;
	MQTT.soc_func  = func;
	MQTT.pBuffer   = dst;
	MQTT.maxSize   = size;
	MQTT.timeout   = timeout;
	MQTT_SetState( ST_MQTT_SOC_EXECUTE, 1 );
	Ql_OS_WaitEvent( eventId, EVENT_MQTT_EXECUTE ); // BLOCK THIS TASK
	LED_SET( 0 );
	return MQTT.totalSize;
}

#endif // USE_MQTT



