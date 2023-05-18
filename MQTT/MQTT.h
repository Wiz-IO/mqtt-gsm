/*
 * MQTT.h
 *
 *  Created on: 04.10.2016
 *      Author: georgi.angelov
 */

#ifndef MQTT_H_
#define MQTT_H_

#include "ql_type.h"
#include "ql_socket.h"
#include "MQTTPacket.h"
#include "MQTTConnect.h"
#include "SYS_TIMERS.h"

#define MQTT_TICK_INTERVAL				(50) /* mSec */
#define MQTT_CONNECT_TIMEOUT			(10000) /* mSec */

#define MQTT_BUFFER_SIZE				(128)
#define MQTT_TOPIC_SIZE					(32)

#define MQTT_CYCLE_TIMEOUT_MS			(1000)	/* mSec */
#define MQTT_COMMAND_TIMEOUT_MS			(8000)	/* mSec */
#define MAX_PACKET_ID 					(65535)
#define MAX_MESSAGE_HANDLERS 			(1)

typedef enum { QOS0, QOS1, QOS2 } QoS_e;

typedef struct
{
    char qos;
    char retained;
    char dup;
    unsigned short id;
    void *payload;
    int payloadlen;
} MQTTMessage;

typedef struct
{
    MQTTMessage * message;
    MQTTString  * topicName;
} MessageData;

typedef int (*messageHandler)(MessageData*);

typedef struct
{
    const char* topicFilter;
    int (*fp) (MessageData*);
} MessageHandlers_t;

typedef struct
{
	bool isConnected;
    u32 next_packetid;
    MQTTHeader header;
    MessageHandlers_t messageHandlers[MAX_MESSAGE_HANDLERS];
    void (*defaultMessageHandler) (MessageData*);
    u32 keepAliveInterval_ms;
    bool pingOutstanding;
    Timer_t pingTimer;
    s32 socketId;
    u8 ipHex[5];
    u16 port;
    s32	 state;
    long timeout;
    bool  socConnected;
    s32(*soc_func)(s32,u8*,s32);
    u8 * Rx;
    u8 * Tx;
    u8 * pBuffer;
    u32 totalSize;
    u32 maxSize;
    u64 sentCounter;
} MQTTClient_t;

extern MQTTClient_t MQTT;
extern char S_TOPIC[];
extern char C_TOPIC[];
extern char L_TOPIC[];

#define MAX_NO_OF_REMAINING_LENGTH_BYTES 	4

#define MQTT_SEND_MESSAGE(M, P1, P2)		Ql_OS_SendMessage(TASK_SRV, M, P1, P2)

void MQTT_CloseSocket(void);
void MQTT_onSocConnect(s32 errCode);
void MQTT_onSocClose(s32 errCode);
int  MQTT_SocketExecute(s32(*func)(s32,u8*,s32), u8* dst, int size, long timeout);
int  MQTT_Connect(MQTTClient_t * c, MQTTPacket_connectData * options);
int  MQTT_Subscribe(MQTTClient_t * c, char * topicFilter, s32 qos, messageHandler messageHandler);
int  MQTT_Publish(MQTTClient_t * c, char* topicName, MQTTMessage* message);
int  MQTT_Keepalive(MQTTClient_t * c);
int  MQTT_onMessage(MessageData* md);

typedef struct mqtt_mess_t
{
	bool status;
	int error;
	MQTTMessage message;
	char * topic;
	char buff[MQTT_BUFFER_SIZE];
	void (*cb)(struct mqtt_mess_t * m);
} mqtt_mess_t;

void MQTT_heapInit(void);
void MQTT_pushMessage(mqtt_mess_t * m, char * topic, char * txt);
mqtt_mess_t * MQTT_allocMessage(void);

typedef enum
{
	ST_MQTT_IDLE					=  0,
	ST_MQTT_SOC_CONNECT				=  1,
	ST_MQTT_WAIT_SOC_CONNECT,
	ST_MQTT_SOC_EXECUTE,
	ST_MQTT_SOC_ACK
} mqtt_states_e;

void MQTT_SetState(int state, u32 wait);

void MQTT_SocClose(void);

#endif /* MQTT_H_ */
