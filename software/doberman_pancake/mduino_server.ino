// IndustrialShield mduino driver, arduino copy
// message format: *{action}{analog/digital/relay}{zone}.{channel}[ {value}]\r\n
// return format:
// "action" is one of "r" (read) "w" (write)
// "analog/digital/relay" is one of "a" or "d" or "r"
// "channel" is a two- or three-digit number like "01" or "110", corresponding to the labels on the case (ie, I0.12 -> "012")
// "value" is either 1 or 0 for digital lines and relays, or a value in [0, FF] for analog lines (must be hex)
// an example: "*ra04\r\n" reads from A0.4; "*wr2.2 1\r\n" sets relay 2.2 to closed
// 
// Things to note: the pin mapping is written for the 57AAR+, and the mac and ethernet are hard-coded

#if defined(MDUINO_PLUS)
#include <Ethernet2.h>
#else
#include <Ethernet.h>
#endif
//#include <SimpleComm.h>

const uint8_t NC = 0xFF;

const uint8_t relay[9] = {
  NC,
  33, // R2.1
  32, // R2.2
  35, // R2.3
  34, // R2.4
  NC,
  49, // R2.6
  48, // R2.7
  47 // R2.8
};

const uint8_t digital_in[3][13] = {
  {
    22, // I0.0
    23, // I0.1
    24, // I0.2
    25, // I0.3
    36, // I0.4
    2, // I0.5
    3, // I0.6
    54, // I0.7
    55, // I0.8
    56, // I0.9
    57, // I0.10
    58, // I0.11
    59 // I0.12
  },
  {
    27, // I1.0
    28, // I1.1
    29, // I1.2
    30, // I1.3
    31, // I1.4
    18, // I1.5
    19, // I1.6
    60, // I1.7
    61, // I1.8
    62, // I1.9
    63, // I1.10,
    64, // I1.11,
    65 // I1.12
  },
  {
    20, // I2.0
    21, // I2.1
    66, // I2.2
    67, // I2.3
    68, // I2.4
    69, // I2.5
    NC,
    NC,
    NC,
    NC,
    NC,
    NC,
    NC
  }
};

const uint8_t digital_out[3][8] = {
  {
    36, // Q0.0
    37, // Q0.1
    38, // Q0.2
    39, // Q0.3
    40, // Q0.4
    4, // Q0.5 pwm
    5, // Q0.6 pwm
    6 // Q0.7 pwm
  },
  {
    41, // Q1.0
    42, // Q1.1
    43, // Q1.2
    44, // Q1.3
    45, // Q1.4
    8, // Q1.5 pwm
    9, // Q1.6 pwm
    7 // Q1.7 pwm
  },
  {
    12, // Q2.0 pwm
    13, // Q2.1 pwm
    NC,
    NC,
    NC,
    NC,
    NC,
    NC
  }
};

const uint8_t analog_out[2][8] = {
  {
    NC,
    NC,
    NC,
    NC,
    NC,
    4, // A0.5
    5, // A0.6
    6 // A0.7
  },
  {
    NC,
    NC,
    NC,
    NC,
    NC,
    8, // A1.5
    9, // A1.6
    7 // A1.7
  }
};

const uint8_t analog_in[3][13] = {
  {
    NC,
    NC,
    NC,
    NC,
    NC,
    NC,
    54, // I0.7
    55, // I0.8
    56, // I0.9
    57, // I0.10
    58, // I0.11
    59 // I0.12
  },
  {
    NC,
    NC,
    NC,
    NC,
    NC,
    NC,
    60, // I1.7
    61, // I1.8
    62, // I1.9
    63, // I1.10
    64, // I1.11
    65 // I1.12
  },
  {
    NC,
    NC,
    66, // I2.2
    67, // I2.3
    68, // I2.4
    69, // I2.5
    NC,
    NC,
    NC,
    NC,
    NC,
    NC,
    NC
  }
};

/* type map:
  A = analog out
  I = analog in
  i = digital in
  R = relay
  Q = digital out (incl pwm)
  */

// no mac specified, copied this from an example
uint8_t mac[] = {0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0xED};
IPAddress ip(192, 168, 131, 25);
IPAddress nameServer(192, 168, 131, 1);
IPAddress gateway(192, 168, 131, 1);
IPAddress netmask(255, 255, 255, 0);
const int eth_port = 10001;

const int MIN_MESSAGE_LENGTH = strlen("*RI0.1\r\n");
const int MAX_MESSAGE_LENGTH = strlen("*WI0.12 FF\r\n");

EthernetServer server(eth_port);
char packet[MAX_MESSAGE_LENGTH];

void setup() {
  //Serial.begin(9600);
  Ethernet.begin(mac, ip, nameServer, gateway, netmask);
  server.begin();
  //Serial.println("Got IP:");
  //Serial.println(Ethernet.localIP());
  // set pins in various zones to correct mode
  // relay
  for (int i = 0; i < 9; i++) if (relay[i] != NC) pinMode(relay[i], OUTPUT);

  // analog_out
  for (int z = 0; z < 2; z++) for (int p = 0; p < 8; p++) if (analog_out[z][p] != NC) pinMode(analog_out[z][p], OUTPUT);

  // analog_in
  for (int z = 0; z < 3; z++) for (int p = 0; p < 13; p++) if (analog_in[z][p] != NC) pinMode(analog_in[z][p], INPUT);

  // digital_out
  for (int z = 0; z < 3; z++) for (int p = 0; p < 8; p++) if (digital_out[z][p] != NC) pinMode(digital_out[z][p], OUTPUT);

  // digital_in
  for (int z = 0; z < 3; z++) for (int p = 0; p < 13; p++) if (digital_in[z][p] != NC) pinMode(digital_in[z][p], INPUT);
}

void loop() {
  EthernetClient client = server.available();
  if (client) {
    //Serial.println("Got client");
    int j = 0;
    //int cmd_start = millis();
    while (client.available() && j < MAX_MESSAGE_LENGTH) {
      packet[j++] = client.read();
      //Serial.println("Read char");
      if (packet[j-1] == '\n')
        break;
    }
    //Serial.println(millis()-cmd_start);
    //Serial.println("Recieved packet");
    //Serial.println(packet);
    //Serial.println(j);
    if (MIN_MESSAGE_LENGTH <= j && j <= MAX_MESSAGE_LENGTH && packet[0] == '*') {

      // 0123456
      // *RI0.4\r\n
      // *RI0.12\r\n
      // *WR2.6 1\r\n
      // *WQ0.12 FF\r\n
      // 0123456789A
      uint8_t action = packet[1];
      uint8_t type = packet[2];
      uint8_t zone = packet[3] - '0';
      // dot = packet[4];
      uint8_t channel = 0;
      if (packet[6] == '\r' || packet[6] == ' ') {
        // one digit channel
        channel = packet[5] - '0';
      } else {
        // two-digit channel
        channel = 10 * (packet[5]-'0') + (packet[6] - '0');
      }
      //Serial.println(millis()-cmd_start);
      int value = 0;
      if (action == 'R') {
        //Serial.println("Read");
        switch(type){
          case 'A': value = analogRead(analog_out[zone][channel]); break;
          case 'I': value = analogRead(analog_in[zone][channel]); break;
          case 'i': value = digitalRead(digital_in[zone][channel]); break;
          case 'Q': value = digitalRead(digital_out[zone][channel]); break;
          case 'R': value = digitalRead(relay[channel]); break;
          default: value=0xFFFF; break;
        }
      } else if (action == 'W') {
        int8_t shift = channel/10;
        if (packet[8+shift] == '\r')
          value = packet[7+shift] - '0';
        else
          value = 10*(packet[7+shift]-'0') + (packet[8+shift]-'0');
        switch(type) {
          case 'A': analogWrite(analog_out[zone][channel], value); break;
          case 'Q': digitalWrite(digital_out[zone][channel], value); break;
          case 'R': digitalWrite(relay[channel], value); break;
          default: value=0xFFFF; break;
        }
      } else {
        // fail
      }
      //Serial.println(millis()-cmd_start);
      char retdata[16];
      sprintf(retdata, "*OK;%d\r\n", value);
      //Serial.println(millis()-cmd_start);
      client.print(retdata);
      //Serial.println(millis()-cmd_start);
    } // if packet is good
    client.stop();
  } // if client
}
