/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef TEST_SSL_CERTIFICATES_HPP
#define TEST_SSL_CERTIFICATES_HPP

#include "test_utils.hpp"

#include <fstream>

namespace test {

// Note: These certificates, keystore, and truststore were auto generated using bin2c

/**
 * Commands used to generate valid embedded files
 *
 * keytool -genkeypair -noprompt -keyalg RSA -validity 36500 \
 *         -alias node \
 *         -keystore ssl/keystore.jks \
 *         -storepass cassandra \
 *         -keypass cassandra \
 *         -ext SAN="IP:127.0.0.1" \
 *         -dname "CN=127.0.0.1, OU=Drivers and Tools, O=DataStax Inc., L=Santa Clara,
 * ST=California, C=US"
 *
 * keytool -exportcert -noprompt \
 *         -alias node \
 *         -keystore ssl/keystore.jks \
 *         -storepass cassandra \
 *         -file ssl/cassandra.crt
 *
 * keytool -exportcert -rfc -noprompt \
 *         -alias node \
 *         -keystore ssl/keystore.jks \
 *         -storepass cassandra \
 *         -file ssl/cassandra.pem
 *
 * keytool -genkeypair -noprompt -keyalg RSA -validity 36500 \
 *         -alias driver \
 *         -keystore ssl/keystore-driver.jks \
 *         -storepass cassandra \
 *         -keypass cassandra \
 *         -ext SAN="IP:127.0.0.1" \
 *         -dname "CN=127.0.0.1, OU=Drivers and Tools, O=DataStax Inc., L=Santa Clara,
 * ST=California, C=US"
 *
 * keytool -exportcert -noprompt \
 *         -alias driver \
 *         -keystore ssl/keystore-driver.jks \
 *         -storepass cassandra \
 *         -file ssl/cassandra-driver.crt
 *
 * keytool -exportcert -rfc -noprompt \
 *         -alias driver \
 *         -keystore ssl/keystore-driver.jks \
 *         -storepass cassandra \
 *         -file ssl/driver.pem
 *
 * keytool -importkeystore -noprompt -srcalias certificatekey -deststoretype PKCS12 \
 *         -srcalias driver \
 *         -srckeystore ssl/keystore-driver.jks \
 *         -srcstorepass cassandra \
 *         -storepass cassandra \
 *         -destkeystore ssl/keystore-driver.p12
 *
 * #Tail is required to remove additional header information from PEM
 * openssl pkcs12 -nomacver -nocerts -nodes \
 *         -in ssl/keystore-driver.p12 \
 *         -password pass:cassandra | \
 *         tail -n +4 > ssl/driver-private.pem
 *
 * #Encrypt the private key with a password
 * chmod 600 ssl/driver-private.pem
 * ssh-keygen -p \
 *         -N driver \
 *         -f ssl/driver-private.pem
 *
 * keytool -import -noprompt \
 *         -alias truststore \
 *         -keystore ssl/truststore.jks \
 *         -storepass cassandra \
 *         -file ssl/cassandra-driver.crt
 *
 *
 * Commands used to generate invalid embedded files
 *
 * #Cassandra (Peer)
 * keytool -genkeypair -noprompt -keyalg RSA -validity 36500 \
 *         -alias invalid \
 *         -keystore ssl/invalid/keystore-invalid.jks \
 *         -storepass invalid \
 *         -keypass invalid \
 *         -ext SAN="DNS:INVALID" \
 *         -dname "CN=INVALID, OU=INVALID, O=INVALID, L=INVALID, ST=INVALID, C=INVALID"
 * keytool -exportcert -rfc -noprompt \
 *         -alias invalid \
 *         -keystore ssl/invalid/keystore-invalid.jks \
 *         -storepass invalid \
 *         -file ssl/invalid/cassandra-invalid.pem
 *
 * #Driver (Client)
 * keytool -genkeypair -noprompt -keyalg RSA -validity 36500 \
 *         -alias driver-invalid \
 *         -keystore ssl/invalid/keystore-driver-invalid.jks \
 *         -storepass invalid \
 *         -keypass invalid \
 *         -ext SAN="DNS:DRIVER-INVALID" \
 *         -dname "CN=DRIVER-INVALID, OU=DRIVER-INVALID, O=DRIVER-INVALID, L=DRIVER-INVALID,
 * ST=DRIVER-INVALID, C=DRIVER-INVALID"
 * keytool -exportcert -rfc -noprompt \
 *         -alias driver-invalid \
 *         -keystore ssl/invalid/keystore-driver-invalid.jks \
 *         -storepass invalid \
 *         -file ssl/invalid/driver-invalid.pem
 * keytool -importkeystore -noprompt -srcalias certificatekey -deststoretype PKCS12 \
 *         -srcalias driver-invalid \
 *         -srckeystore ssl/invalid/keystore-driver-invalid.jks \
 *         -srcstorepass invalid \
 *         -storepass invalid \
 *         -destkeystore ssl/invalid/keystore-driver-invalid.p12
 * openssl pkcs12 -nomacver -nocerts -nodes \
 *         -in ssl/invalid/keystore-driver-invalid.p12 \
 *         -password pass:invalid | \
 *         tail -n +4 > ssl/invalid/driver-private-invalid.pem
 * chmod 600 ssl/invalid/driver-private-invalid.pem
 * ssh-keygen -p \
 *         -N invalid \
 *         -f ssl/invalid/driver-private-invalid.pem
 */

static const unsigned char cassandra_crt[953] = {
  '0',  0202, 03,   0264, '0',  0202, 02,   0234, 0240, 03,   02,   01,   02,   02,   04,   034,
  0223, 0212, 'W',  '0',  015,  06,   011,  '*',  0206, 'H',  0206, 0367, 015,  01,   01,   013,
  05,   0,    '0',  0201, 0200, '1',  013,  '0',  011,  06,   03,   'U',  04,   06,   023,  02,
  'U',  'S',  '1',  023,  '0',  021,  06,   03,   'U',  04,   010,  023,  012,  'C',  'a',  'l',
  'i',  'f',  'o',  'r',  'n',  'i',  'a',  '1',  024,  '0',  022,  06,   03,   'U',  04,   07,
  023,  013,  'S',  'a',  'n',  't',  'a',  ' ',  'C',  'l',  'a',  'r',  'a',  '1',  026,  '0',
  024,  06,   03,   'U',  04,   012,  023,  015,  'D',  'a',  't',  'a',  'S',  't',  'a',  'x',
  ' ',  'I',  'n',  'c',  '.',  '1',  032,  '0',  030,  06,   03,   'U',  04,   013,  023,  021,
  'D',  'r',  'i',  'v',  'e',  'r',  's',  ' ',  'a',  'n',  'd',  ' ',  'T',  'o',  'o',  'l',
  's',  '1',  022,  '0',  020,  06,   03,   'U',  04,   03,   023,  011,  '1',  '2',  '7',  '.',
  '0',  '.',  '0',  '.',  '1',  '0',  ' ',  027,  015,  '1',  '4',  '1',  '1',  '0',  '6',  '2',
  '1',  '0',  '7',  '2',  '5',  'Z',  030,  017,  '2',  '1',  '1',  '4',  '1',  '0',  '1',  '3',
  '2',  '1',  '0',  '7',  '2',  '5',  'Z',  '0',  0201, 0200, '1',  013,  '0',  011,  06,   03,
  'U',  04,   06,   023,  02,   'U',  'S',  '1',  023,  '0',  021,  06,   03,   'U',  04,   010,
  023,  012,  'C',  'a',  'l',  'i',  'f',  'o',  'r',  'n',  'i',  'a',  '1',  024,  '0',  022,
  06,   03,   'U',  04,   07,   023,  013,  'S',  'a',  'n',  't',  'a',  ' ',  'C',  'l',  'a',
  'r',  'a',  '1',  026,  '0',  024,  06,   03,   'U',  04,   012,  023,  015,  'D',  'a',  't',
  'a',  'S',  't',  'a',  'x',  ' ',  'I',  'n',  'c',  '.',  '1',  032,  '0',  030,  06,   03,
  'U',  04,   013,  023,  021,  'D',  'r',  'i',  'v',  'e',  'r',  's',  ' ',  'a',  'n',  'd',
  ' ',  'T',  'o',  'o',  'l',  's',  '1',  022,  '0',  020,  06,   03,   'U',  04,   03,   023,
  011,  '1',  '2',  '7',  '.',  '0',  '.',  '0',  '.',  '1',  '0',  0202, 01,   '"',  '0',  015,
  06,   011,  '*',  0206, 'H',  0206, 0367, 015,  01,   01,   01,   05,   0,    03,   0202, 01,
  017,  0,    '0',  0202, 01,   012,  02,   0202, 01,   01,   0,    0327, 0272, 0354, 0,    01,
  0367, 'c',  0211, 0325, 't',  '6',  024,  '~',  0344, 0214, ')',  0354, 's',  0270, 0342, ';',
  'g',  '(',  0370, 'G',  0307, 0371, 'C',  '%',  '0',  ';',  ' ',  0264, 0355, '}',  's',  0245,
  'R',  0247, '(',  'z',  0323, 0335, 0245, 0244, 0326, 0324, 'e',  'k',  0224, 0250, 0270, 0263,
  0270, 'Z',  024,  0246, 0240, 'w',  'Q',  '/',  'S',  010,  0217, 027,  0364, 0275, 0372, 'k',
  0134, 0216, '2',  0222, 0245, ')',  0304, 0265, 'V',  '/',  'R',  015,  '%',  0237, '-',  'i',
  0134, 'E',  0377, 027,  'j',  '5',  'L',  0202, '8',  '=',  0311, 0321, 0237, '+',  0267, 0212,
  0214, 0203, 'Q',  'I',  0344, 0256, 0214, 'z',  0326, 'l',  0343, 0353, 'D',  '8',  'C',  'L',
  0325, 04,   01,   0322, 0237, 0211, 'O',  '{',  0221, 0350, 0341, 0245, 0257, 0236, '$',  0304,
  0361, 0334, 0341, 0323, '/',  0340, 0354, 'L',  'T',  'a',  0320, 0267, 0324, 's',  0367, 'b',
  0355, 0262, 0356, 0326, '^',  '_',  0255, 0245, 'T',  'w',  '3',  0210, 0302, 0341, 0350, 'v',
  'W',  'i',  0223, '[',  0134, 0323, 0333, 0210, 0355, 'o',  0320, 0255, ')',  0313, 0245, 0231,
  015,  0345, 0233, 'u',  034,  '(',  0367, 0230, 'g',  'y',  0331, 0276, 'C',  0333, 'V',  0262,
  0363, 0235, 0265, 'n',  0372, 0214, ' ',  0377, 01,   0305, 037,  0275, 'a',  0356, 0250, '5',
  0250, 0251, 0327, 0350, 0370, '7',  'k',  020,  'E',  0335, '!',  0351, '>',  's',  0276, 0354,
  0221, ']',  '<',  0270, 0213, 0214, 'u',  016,  0361, '8',  '/',  'z',  0217, 0333, '~',  0236,
  't',  '3',  0207, 07,   'L',  013,  0,    034,  0205, 024,  'k',  02,   03,   01,   0,    01,
  0243, '2',  '0',  '0',  '0',  017,  06,   03,   'U',  035,  021,  04,   010,  '0',  06,   0207,
  04,   0177, 0,    0,    01,   '0',  035,  06,   03,   'U',  035,  016,  04,   026,  04,   024,
  0230, 0201, 'I',  0271, 0273, 0316, 'e',  'T',  0252, '[',  0245, 0307, 0304, 0134, '$',  0241,
  0304, 0223, 0222, 'E',  '0',  015,  06,   011,  '*',  0206, 'H',  0206, 0367, 015,  01,   01,
  013,  05,   0,    03,   0202, 01,   01,   0,    0306, 0223, 0303, 0234, 037,  ';',  0223, 'v',
  047,  0201, 0305, 0307, 0226, 013,  0356, 0212, 01,   '+',  07,   'u',  'm',  'M',  '_',  'Q',
  '=',  0320, 0220, 06,   ',',  0226, '_',  0340, 0355, 'b',  'G',  'J',  'K',  0267, 0225, 0310,
  0315, '%',  0134, 0334, 'j',  '^',  0253, 0245, 02,   011,  'Y',  ',',  '&',  ':',  0310, 0246,
  0243, '!',  'A',  '~',  '.',  0365, 0242, 0204, 0323, 0221, 'X',  0343, 'V',  0200, 011,  'h',
  0344, 'E',  0230, 'O',  0353, 's',  'K',  0347, 'J',  0262, 0342, '!',  0246, 01,   014,  0367,
  0311, 0213, 0325, 0270, '3',  0250, 034,  015,  'i',  'S',  0207, 0225, 0255, 0264, 06,   '6',
  'S',  'V',  0261, '+',  0203, 'e',  '^',  0354, 'F',  '2',  'B',  0314, 04,   0374, '5',  'Z',
  0326, '+',  0347, 0204, 0205, 0214, 0333, 0233, 0364, 0330, '7',  023,  0362, 0177, 0237, 'Q',
  0336, 'E',  0376, 0232, 'k',  '/',  047,  'F',  0216, '-',  04,   0226, 's',  'C',  '4',  07,
  0201, ',',  '~',  0340, 0323, 0267, 0330, 0272, 0237, 0270, 0236, 0357, '#',  011,  0206, 'J',
  'k',  0270, 0220, 020,  0362, 011,  0222, 0273, 0211, 0311, 0203, 'k',  '~',  'H',  0333, 0231,
  'q',  0244, '#',  0246, 0241, '7',  023,  0265, '4',  0357, 0224, 0361, 01,   0271, 0246, 0374,
  0323, 036,  0255, ':',  '!',  'O',  'i',  'S',  0315, 04,   015,  0244, 0304, 0261, 'F',  ' ',
  'l',  'z',  '2',  0237, 0263, 0301, 'D',  0241, 0263, 0336, 017,  0217, 0317, 'l',  'v',  0257,
  'z',  0336, 'd',  0257, 01,   '-',  0333, 0357, 0234, 0302, 0367, 'W',  0367, 'N',  0372, 0326,
  'x',  0231, 0215, 0302, 'J',  '_',  '+',  'U',  012,
};
static const unsigned char keystore_jks[2304] = {
  0376, 0355, 0376, 0355, 0,    0,    0,    02,   0,    0,    0,    01,   0,    0,    0,    01,
  0,    04,   'n',  'o',  'd',  'e',  0,    0,    01,   'I',  0206, 0356, 0354, 030,  0,    0,
  05,   02,   '0',  0202, 04,   0376, '0',  016,  06,   012,  '+',  06,   01,   04,   01,   '*',
  02,   021,  01,   01,   05,   0,    04,   0202, 04,   0352, 0340, 't',  '9',  0301, '8',  0261,
  034,  'h',  0240, 0360, 0341, 0301, 0345, 0325, 'h',  0214, 'G',  0316, ';',  't',  '"',  05,
  '8',  0376, 032,  0217, 031,  032,  0204, 'q',  020,  'T',  0271, ']',  0263, ')',  0265, 0330,
  'h',  0300, 0261, 0306, 0257, 0211, 035,  0271, 0205, '$',  023,  0223, '4',  02,   'G',  'q',
  0265, 'u',  0205, 0251, 0214, 0237, 0373, 'I',  0237, 0233, 0225, '>',  0254, '(',  'X',  '3',
  'W',  '{',  '*',  0202, '@',  0271, 0361, 016,  037,  'j',  '.',  0231, ',',  'H',  0277, '"',
  0324, 0277, 0222, ':',  0313, '2',  'J',  'o',  0324, 'w',  023,  0221, 0206, 030,  '}',  0206,
  0352, 023,  0360, '9',  'K',  ']',  0212, 'g',  'V',  '|',  'V',  'g',  'M',  0255, 0372, 020,
  025,  '2',  '`',  0217, '1',  0371, 0216, ' ',  0276, '^',  '-',  047,  0347, 013,  '^',  '%',
  0237, 'y',  0362, 'i',  0177, 'o',  'Y',  'G',  '+',  0335, 'M',  025,  'C',  0336, 037,  014,
  'Y',  'M',  020,  '$',  035,  'g',  0361, 'o',  026,  0345, 'H',  0375, '?',  0363, 'w',  012,
  '+',  0323, 'c',  0343, 'i',  034,  0361, '0',  024,  0267, 'U',  0242, 0255, 027,  'b',  0263,
  037,  011,  0225, 0231, 0341, '-',  ')',  0212, 0260, 'm',  02,   025,  0314, 'I',  'm',  0262,
  0373, 0323, 0264, 0345, 0134, 0376, 0337, 0324, 030,  'n',  'a',  017,  ',',  '}',  0203, 0263,
  'w',  '?',  0234, 'v',  0232, 'B',  0222, 0356, 0317, 0356, 0231, '(',  0355, 015,  'a',  02,
  '=',  0216, 0271, ',',  'o',  0270, 0276, 0303, 'G',  'F',  010,  'N',  '=',  026,  0214, 0263,
  ',',  'U',  '$',  0306, 0322, '[',  0266, 026,  0177, 'I',  0310, 0305, 0254, 0305, '4',  ')',
  0372, 0262, 025,  0331, 'D',  0345, 'R',  05,   0305, 0367, 0256, '`',  '}',  0201, 'e',  0237,
  'b',  0261, 'z',  'O',  0276, '|',  0315, 0365, '7',  0206, '?',  '5',  0,    'M',  'u',  0276,
  '*',  0304, 0211, 0343, 026,  'X',  0266, 0227, 026,  0301, 0333, '&',  'K',  '*',  0306, 0134,
  0205, '#',  037,  0267, 'S',  '|',  0275, 05,   0216, '1',  02,   0241, 'v',  0250, 'W',  0320,
  0225, 0247, 'b',  0332, '^',  0370, 015,  'G',  'R',  015,  0343, 024,  0327, 0210, 'I',  0367,
  '0',  'H',  0377, 0245, '2',  0326, '~',  0226, 'q',  0230, 'j',  0317, 'L',  0251, 'd',  03,
  0262, '{',  0246, 0273, 0243, 0374, 0256, 's',  '4',  0322, 0313, 0226, 0203, 0246, 0304, 0203,
  0267, 0341, 'i',  't',  0300, 'x',  '9',  03,   0333, '`',  0276, 0253, 'z',  'K',  0344, '6',
  037,  0212, 'U',  0253, 0214, 0231, 0347, 034,  '-',  0353, 'Q',  0346, 'X',  0277, 0200, 0222,
  0222, 0215, 0261, 0307, 0345, 0276, 'd',  'o',  ']',  'J',  027,  'A',  'b',  'i',  0210, 's',
  06,   0220, 0313, 0205, ',',  0337, 04,   'M',  0352, 0236, 0304, 030,  'G',  0315, 0274, 0247,
  0354, 'p',  0355, 025,  0335, 'R',  '>',  0224, 0353, 'O',  0254, '-',  'R',  'Z',  '(',  0260,
  '=',  ':',  0206, 0334, '}',  '^',  'k',  '1',  0352, 0357, 0241, 0214, 0233, 0245, '|',  'Y',
  0302, '9',  01,   0201, 0374, 'V',  010,  0177, 'F',  0346, 0361, 0300, 0315, ')',  021,  0315,
  'K',  0332, 'l',  0311, 0352, 0223, 'U',  'm',  '"',  '.',  0307, 0202, 07,   '~',  0345, 0375,
  'D',  0326, 0251, 0256, 0244, 0213, 0306, 0250, 0244, 010,  012,  010,  '9',  'u',  0275, '%',
  047,  ']',  0303, 'N',  '6',  'I',  0336, 026,  'y',  0337, 0247, 'k',  0240, '`',  0244, 'x',
  'p',  0370, 0202, 0301, 0351, '%',  035,  '8',  't',  0271, 0322, 0275, 'c',  0236, 'i',  0355,
  0370, 0212, 0311, 0325, '+',  '5',  0202, 0266, ',',  0360, 0327, 0305, 031,  0233, 0246, 0242,
  032,  'y',  0232, 0241, 0271, 0212, 0346, 0360, 0306, '_',  027,  0303, 'F',  0311, 'D',  036,
  0220, 026,  '"',  't',  'u',  032,  'V',  0317, 0205, 0250, 'F',  '~',  'N',  03,   0320, 0244,
  0362, 0220, 0270, 'J',  '&',  0273, ',',  0347, 0221, 04,   0327, 'g',  'h',  0223, 0275, 0334,
  'M',  'Z',  't',  022,  'Z',  033,  'H',  0222, '}',  03,   011,  0266, '0',  'A',  0237, '-',
  0320, 'b',  0331, ';',  '[',  0214, 0225, ';',  0342, 0316, 0242, 0224, 'o',  'o',  0341, 'b',
  023,  '8',  'O',  0244, 0316, 0207, '!',  0335, 'U',  0341, 020,  0321, 010,  0333, 0230, 0200,
  'W',  0340, 0134, 0251, 0212, 013,  'p',  ':',  015,  'd',  015,  024,  0214, '.',  'i',  '&',
  0350, 0341, 0302, 0231, 'V',  0202, 0202, 'z',  'P',  0342, 'f',  0210, 0243, 'Q',  0325, 0257,
  'C',  'r',  ' ',  0331, 0364, 025,  0267, 0356, 0244, 'T',  0316, 035,  0235, 023,  0312, 0212,
  '#',  0375, 'e',  0364, 0301, 04,   '7',  0374, 0372, '#',  012,  '-',  0235, 04,   '(',  0341,
  023,  0302, 012,  0225, ':',  0134, 'S',  '%',  032,  0361, 0213, 'e',  ',',  'q',  0337, 0324,
  0205, 'Y',  0303, 034,  0263, 0220, '6',  0241, 0363, 0236, 0366, '/',  0206, 0276, 'a',  0310,
  0376, 0233, 0376, 'z',  0240, 'y',  0344, 0220, 030,  '}',  '6',  '-',  0271, 0347, 0325, '#',
  'e',  0363, 's',  'F',  0227, 021,  030,  '}',  '=',  0350, '!',  0300, ',',  0202, 0224, '>',
  0250, 'E',  0303, 0300, 0306, 'L',  0332, 0313, 'z',  0241, 'v',  036,  0244, 'k',  'V',  0203,
  011,  0343, 0367, 'L',  0227, 0261, '~',  0254, 027,  '8',  0304, 'K',  0354, 0363, 0325, 0317,
  'r',  0266, 0375, 025,  02,   0344, '0',  05,   'f',  ':',  0276, 'N',  0212, 'e',  0207, 0204,
  '^',  0213, 'b',  0265, '@',  '%',  024,  'v',  '{',  '1',  0246, 017,  0215, 014,  0255, 0250,
  '8',  0314, '4',  0301, 0376, 0366, 011,  0326, 0332, 0213, 036,  '+',  0335, 0212, 0277, 0221,
  0263, '4',  0276, 'G',  'O',  010,  024,  0371, 0351, '^',  0250, 0364, 'z',  0247, 0177, 0177,
  0220, 016,  0217, 0303, 0254, 'T',  'Y',  010,  '<',  0235, 030,  0266, 0305, 0356, ',',  0217,
  'O',  0334, 0325, 02,   0247, 'f',  'a',  0222, '/',  0203, ']',  '~',  0233, 0314, 0363, 0276,
  '+',  0374, 047,  0373, '7',  '}',  '/',  'N',  0270, 0257, 0360, 0241, '1',  0323, 0264, 0254,
  0241, 'u',  '*',  0371, 014,  021,  0333, 'd',  'N',  'l',  0232, 0223, '2',  0302, '%',  0241,
  '0',  0357, 0277, '{',  '.',  0375, 0333, 0351, '!',  023,  0247, 's',  0371, 0322, 0313, 0336,
  0245, 04,   'O',  0320, '[',  0222, 'N',  0342, 0363, 015,  0323, 02,   0304, 0270, 0215, 0322,
  0350, 0303, ':',  'l',  0314, 021,  0305, 0211, 0275, 'T',  'i',  0256, 0276, 013,  020,  0351,
  0226, 012,  ' ',  0264, 0,    '{',  0304, 0134, 'Y',  0214, 0223, '=',  0271, 0355, 026,  'E',
  014,  'D',  ' ',  024,  0300, 'f',  0344, 's',  0253, 0322, 'u',  ']',  0341, '*',  031,  0257,
  0262, 0374, 0376, 0235, 0315, 012,  ')',  'c',  0326, '/',  0327, 0344, 0223, 0224, 'n',  037,
  'c',  0324, 0206, 0325, 0273, 0254, 0222, 0302, 'G',  0230, ')',  027,  'i',  034,  0237, '*',
  'S',  '-',  0215, 0277, 0277, 0370, 014,  015,  0232, '3',  'z',  ',',  'j',  0347, 'M',  06,
  'H',  0222, '5',  0205, 0247, 0267, '%',  '-',  0205, 'L',  0272, 0331, 0335, 017,  0207, 0266,
  'E',  0204, 0206, 0200, 0314, 0342, 0345, '1',  0234, 0263, 0222, 'M',  0313, 0240, '}',  0234,
  'V',  0210, 0311, 032,  'u',  '6',  0231, '(',  024,  0254, 0207, 012,  '>',  027,  0204, ':',
  'R',  0373, 0213, '{',  ',',  0313, 024,  012,  '?',  034,  0333, 0357, 0304, 0274, '2',  0340,
  'N',  '~',  '~',  0316, 0254, 0344, 01,   '1',  0134, ':',  'A',  ';',  0252, 0256, '7',  'L',
  0263, 0330, 0357, 0302, 024,  0376, 'g',  '!',  0344, 0324, 'Q',  '7',  'b',  '|',  013,  03,
  0303, 0216, 0330, 0370, 's',  'l',  0247, 0350, 0330, 0366, 'f',  0310, ',',  0240, 'p',  0224,
  0302, 0325, 'Y',  0357, ',',  0356, 0335, 0365, 'c',  0205, 0265, 0247, 'H',  0220, 0227, 0240,
  0236, 07,   0354, 0377, ':',  02,   0245, 0317, 0351, 'S',  0305, 0252, 0352, 0344, '}',  'f',
  0363, '4',  0254, 'l',  0210, '$',  'z',  '6',  0334, '6',  '.',  0300, 032,  '^',  0225, ' ',
  '{',  0307, ':',  'W',  0243, 0313, 0335, 'P',  0250, 0236, 0350, 0306, 026,  0344, 0217, 0375,
  047,  '6',  '>',  0237, 0,    0,    0,    01,   0,    05,   'X',  '.',  '5',  '0',  '9',  0,
  0,    03,   0270, '0',  0202, 03,   0264, '0',  0202, 02,   0234, 0240, 03,   02,   01,   02,
  02,   04,   034,  0223, 0212, 'W',  '0',  015,  06,   011,  '*',  0206, 'H',  0206, 0367, 015,
  01,   01,   013,  05,   0,    '0',  0201, 0200, '1',  013,  '0',  011,  06,   03,   'U',  04,
  06,   023,  02,   'U',  'S',  '1',  023,  '0',  021,  06,   03,   'U',  04,   010,  023,  012,
  'C',  'a',  'l',  'i',  'f',  'o',  'r',  'n',  'i',  'a',  '1',  024,  '0',  022,  06,   03,
  'U',  04,   07,   023,  013,  'S',  'a',  'n',  't',  'a',  ' ',  'C',  'l',  'a',  'r',  'a',
  '1',  026,  '0',  024,  06,   03,   'U',  04,   012,  023,  015,  'D',  'a',  't',  'a',  'S',
  't',  'a',  'x',  ' ',  'I',  'n',  'c',  '.',  '1',  032,  '0',  030,  06,   03,   'U',  04,
  013,  023,  021,  'D',  'r',  'i',  'v',  'e',  'r',  's',  ' ',  'a',  'n',  'd',  ' ',  'T',
  'o',  'o',  'l',  's',  '1',  022,  '0',  020,  06,   03,   'U',  04,   03,   023,  011,  '1',
  '2',  '7',  '.',  '0',  '.',  '0',  '.',  '1',  '0',  ' ',  027,  015,  '1',  '4',  '1',  '1',
  '0',  '6',  '2',  '1',  '0',  '7',  '2',  '5',  'Z',  030,  017,  '2',  '1',  '1',  '4',  '1',
  '0',  '1',  '3',  '2',  '1',  '0',  '7',  '2',  '5',  'Z',  '0',  0201, 0200, '1',  013,  '0',
  011,  06,   03,   'U',  04,   06,   023,  02,   'U',  'S',  '1',  023,  '0',  021,  06,   03,
  'U',  04,   010,  023,  012,  'C',  'a',  'l',  'i',  'f',  'o',  'r',  'n',  'i',  'a',  '1',
  024,  '0',  022,  06,   03,   'U',  04,   07,   023,  013,  'S',  'a',  'n',  't',  'a',  ' ',
  'C',  'l',  'a',  'r',  'a',  '1',  026,  '0',  024,  06,   03,   'U',  04,   012,  023,  015,
  'D',  'a',  't',  'a',  'S',  't',  'a',  'x',  ' ',  'I',  'n',  'c',  '.',  '1',  032,  '0',
  030,  06,   03,   'U',  04,   013,  023,  021,  'D',  'r',  'i',  'v',  'e',  'r',  's',  ' ',
  'a',  'n',  'd',  ' ',  'T',  'o',  'o',  'l',  's',  '1',  022,  '0',  020,  06,   03,   'U',
  04,   03,   023,  011,  '1',  '2',  '7',  '.',  '0',  '.',  '0',  '.',  '1',  '0',  0202, 01,
  '"',  '0',  015,  06,   011,  '*',  0206, 'H',  0206, 0367, 015,  01,   01,   01,   05,   0,
  03,   0202, 01,   017,  0,    '0',  0202, 01,   012,  02,   0202, 01,   01,   0,    0327, 0272,
  0354, 0,    01,   0367, 'c',  0211, 0325, 't',  '6',  024,  '~',  0344, 0214, ')',  0354, 's',
  0270, 0342, ';',  'g',  '(',  0370, 'G',  0307, 0371, 'C',  '%',  '0',  ';',  ' ',  0264, 0355,
  '}',  's',  0245, 'R',  0247, '(',  'z',  0323, 0335, 0245, 0244, 0326, 0324, 'e',  'k',  0224,
  0250, 0270, 0263, 0270, 'Z',  024,  0246, 0240, 'w',  'Q',  '/',  'S',  010,  0217, 027,  0364,
  0275, 0372, 'k',  0134, 0216, '2',  0222, 0245, ')',  0304, 0265, 'V',  '/',  'R',  015,  '%',
  0237, '-',  'i',  0134, 'E',  0377, 027,  'j',  '5',  'L',  0202, '8',  '=',  0311, 0321, 0237,
  '+',  0267, 0212, 0214, 0203, 'Q',  'I',  0344, 0256, 0214, 'z',  0326, 'l',  0343, 0353, 'D',
  '8',  'C',  'L',  0325, 04,   01,   0322, 0237, 0211, 'O',  '{',  0221, 0350, 0341, 0245, 0257,
  0236, '$',  0304, 0361, 0334, 0341, 0323, '/',  0340, 0354, 'L',  'T',  'a',  0320, 0267, 0324,
  's',  0367, 'b',  0355, 0262, 0356, 0326, '^',  '_',  0255, 0245, 'T',  'w',  '3',  0210, 0302,
  0341, 0350, 'v',  'W',  'i',  0223, '[',  0134, 0323, 0333, 0210, 0355, 'o',  0320, 0255, ')',
  0313, 0245, 0231, 015,  0345, 0233, 'u',  034,  '(',  0367, 0230, 'g',  'y',  0331, 0276, 'C',
  0333, 'V',  0262, 0363, 0235, 0265, 'n',  0372, 0214, ' ',  0377, 01,   0305, 037,  0275, 'a',
  0356, 0250, '5',  0250, 0251, 0327, 0350, 0370, '7',  'k',  020,  'E',  0335, '!',  0351, '>',
  's',  0276, 0354, 0221, ']',  '<',  0270, 0213, 0214, 'u',  016,  0361, '8',  '/',  'z',  0217,
  0333, '~',  0236, 't',  '3',  0207, 07,   'L',  013,  0,    034,  0205, 024,  'k',  02,   03,
  01,   0,    01,   0243, '2',  '0',  '0',  '0',  017,  06,   03,   'U',  035,  021,  04,   010,
  '0',  06,   0207, 04,   0177, 0,    0,    01,   '0',  035,  06,   03,   'U',  035,  016,  04,
  026,  04,   024,  0230, 0201, 'I',  0271, 0273, 0316, 'e',  'T',  0252, '[',  0245, 0307, 0304,
  0134, '$',  0241, 0304, 0223, 0222, 'E',  '0',  015,  06,   011,  '*',  0206, 'H',  0206, 0367,
  015,  01,   01,   013,  05,   0,    03,   0202, 01,   01,   0,    0306, 0223, 0303, 0234, 037,
  ';',  0223, 'v',  047,  0201, 0305, 0307, 0226, 013,  0356, 0212, 01,   '+',  07,   'u',  'm',
  'M',  '_',  'Q',  '=',  0320, 0220, 06,   ',',  0226, '_',  0340, 0355, 'b',  'G',  'J',  'K',
  0267, 0225, 0310, 0315, '%',  0134, 0334, 'j',  '^',  0253, 0245, 02,   011,  'Y',  ',',  '&',
  ':',  0310, 0246, 0243, '!',  'A',  '~',  '.',  0365, 0242, 0204, 0323, 0221, 'X',  0343, 'V',
  0200, 011,  'h',  0344, 'E',  0230, 'O',  0353, 's',  'K',  0347, 'J',  0262, 0342, '!',  0246,
  01,   014,  0367, 0311, 0213, 0325, 0270, '3',  0250, 034,  015,  'i',  'S',  0207, 0225, 0255,
  0264, 06,   '6',  'S',  'V',  0261, '+',  0203, 'e',  '^',  0354, 'F',  '2',  'B',  0314, 04,
  0374, '5',  'Z',  0326, '+',  0347, 0204, 0205, 0214, 0333, 0233, 0364, 0330, '7',  023,  0362,
  0177, 0237, 'Q',  0336, 'E',  0376, 0232, 'k',  '/',  047,  'F',  0216, '-',  04,   0226, 's',
  'C',  '4',  07,   0201, ',',  '~',  0340, 0323, 0267, 0330, 0272, 0237, 0270, 0236, 0357, '#',
  011,  0206, 'J',  'k',  0270, 0220, 020,  0362, 011,  0222, 0273, 0211, 0311, 0203, 'k',  '~',
  'H',  0333, 0231, 'q',  0244, '#',  0246, 0241, '7',  023,  0265, '4',  0357, 0224, 0361, 01,
  0271, 0246, 0374, 0323, 036,  0255, ':',  '!',  'O',  'i',  'S',  0315, 04,   015,  0244, 0304,
  0261, 'F',  ' ',  'l',  'z',  '2',  0237, 0263, 0301, 'D',  0241, 0263, 0336, 017,  0217, 0317,
  'l',  'v',  0257, 'z',  0336, 'd',  0257, 01,   '-',  0333, 0357, 0234, 0302, 0367, 'W',  0367,
  'N',  0372, 0326, 'x',  0231, 0215, 0302, 'J',  '_',  '+',  'U',  '+',  0355, '}',  'p',  'u',
  '8',  0223, 06,   '~',  0364, 0327, 'J',  '_',  'n',  'W',  0331, 0323, 0347, 03,   'v',  012,
};
static const unsigned char truststore_jks[1020] = {
  0376, 0355, 0376, 0355, 0,    0,    0,    02,   0,    0,    0,    01,   0,    0,    0,    02,
  0,    012,  't',  'r',  'u',  's',  't',  's',  't',  'o',  'r',  'e',  0,    0,    01,   'I',
  0206, 0356, 0375, '9',  0,    05,   'X',  '.',  '5',  '0',  '9',  0,    0,    03,   0270, '0',
  0202, 03,   0264, '0',  0202, 02,   0234, 0240, 03,   02,   01,   02,   02,   04,   031,  0273,
  0237, 0223, '0',  015,  06,   011,  '*',  0206, 'H',  0206, 0367, 015,  01,   01,   013,  05,
  0,    '0',  0201, 0200, '1',  013,  '0',  011,  06,   03,   'U',  04,   06,   023,  02,   'U',
  'S',  '1',  023,  '0',  021,  06,   03,   'U',  04,   010,  023,  012,  'C',  'a',  'l',  'i',
  'f',  'o',  'r',  'n',  'i',  'a',  '1',  024,  '0',  022,  06,   03,   'U',  04,   07,   023,
  013,  'S',  'a',  'n',  't',  'a',  ' ',  'C',  'l',  'a',  'r',  'a',  '1',  026,  '0',  024,
  06,   03,   'U',  04,   012,  023,  015,  'D',  'a',  't',  'a',  'S',  't',  'a',  'x',  ' ',
  'I',  'n',  'c',  '.',  '1',  032,  '0',  030,  06,   03,   'U',  04,   013,  023,  021,  'D',
  'r',  'i',  'v',  'e',  'r',  's',  ' ',  'a',  'n',  'd',  ' ',  'T',  'o',  'o',  'l',  's',
  '1',  022,  '0',  020,  06,   03,   'U',  04,   03,   023,  011,  '1',  '2',  '7',  '.',  '0',
  '.',  '0',  '.',  '1',  '0',  ' ',  027,  015,  '1',  '4',  '1',  '1',  '0',  '6',  '2',  '1',
  '0',  '7',  '2',  '6',  'Z',  030,  017,  '2',  '1',  '1',  '4',  '1',  '0',  '1',  '3',  '2',
  '1',  '0',  '7',  '2',  '6',  'Z',  '0',  0201, 0200, '1',  013,  '0',  011,  06,   03,   'U',
  04,   06,   023,  02,   'U',  'S',  '1',  023,  '0',  021,  06,   03,   'U',  04,   010,  023,
  012,  'C',  'a',  'l',  'i',  'f',  'o',  'r',  'n',  'i',  'a',  '1',  024,  '0',  022,  06,
  03,   'U',  04,   07,   023,  013,  'S',  'a',  'n',  't',  'a',  ' ',  'C',  'l',  'a',  'r',
  'a',  '1',  026,  '0',  024,  06,   03,   'U',  04,   012,  023,  015,  'D',  'a',  't',  'a',
  'S',  't',  'a',  'x',  ' ',  'I',  'n',  'c',  '.',  '1',  032,  '0',  030,  06,   03,   'U',
  04,   013,  023,  021,  'D',  'r',  'i',  'v',  'e',  'r',  's',  ' ',  'a',  'n',  'd',  ' ',
  'T',  'o',  'o',  'l',  's',  '1',  022,  '0',  020,  06,   03,   'U',  04,   03,   023,  011,
  '1',  '2',  '7',  '.',  '0',  '.',  '0',  '.',  '1',  '0',  0202, 01,   '"',  '0',  015,  06,
  011,  '*',  0206, 'H',  0206, 0367, 015,  01,   01,   01,   05,   0,    03,   0202, 01,   017,
  0,    '0',  0202, 01,   012,  02,   0202, 01,   01,   0,    0277, 0317, 'w',  0216, ',',  0303,
  020,  0323, 'V',  0267, 0375, '[',  'T',  '#',  0206, 07,   0233, 0254, '|',  'H',  'I',  't',
  0235, 0367, '|',  0322, 'C',  0,    'w',  037,  'Q',  '$',  'q',  06,   0347, 0222, 020,  0301,
  'p',  0362, 'V',  'j',  '"',  0320, 024,  0236, 0251, 0331, 0350, 0243, 0354, 0207, 0320, 037,
  05,   '"',  0376, 0267, '?',  'i',  0352, '`',  0361, 025,  'a',  'W',  '$',  '/',  '[',  0,
  0203, 0351, '^',  'E',  0303, 0301, 0265, 0251, 0313, 0344, 0320, 021,  'r',  'F',  026,  '|',
  0252, 0266, 0333, 'O',  0366, 0356, 03,   '?',  'i',  0217, ' ',  'X',  0271, 0210, 0306, 0232,
  0213, 'J',  0225, 05,   0371, 0303, 03,   0365, 'n',  0366, 'd',  '8',  0247, 0243, 03,   0347,
  0261, 023,  0177, 0371, 0227, 0260, 'q',  'U',  0323, 0342, 0377, 'v',  'C',  'y',  '{',  0342,
  'l',  0303, '^',  '$',  0235, '%',  0257, 'I',  0335, 0374, 0323, 0374, 'x',  016,  '[',  037,
  0254, 014,  0363, 025,  'j',  0337, 0365, '.',  0222, 0347, 0346, 0260, 0273, 010,  0210, '|',
  0377, '1',  0313, 0235, 'U',  'Z',  0341, 0310, 0220, 024,  037,  0231, 'd',  'S',  0256, 'V',
  '0',  0253, 'F',  'M',  016,  021,  0223, 014,  '(',  036,  0222, 't',  '?',  '3',  0364, 0243,
  '3',  010,  ';',  030,  0256, '7',  014,  'g',  'n',  '"',  '@',  0370, '|',  'p',  'H',  0324,
  'y',  0253, 0237, 'R',  's',  0317, 0255, 0332, 0324, 0357, 0373, 'a',  0243, 0204, 's',  0263,
  0221, 010,  015,  '&',  0240, 0206, 'i',  0211, 0234, 0217, 010,  'D',  0357, 0231, '{',  0350,
  0342, 0212, 0242, 0327, 0322, '6',  0212, 0352, ' ',  017,  02,   03,   01,   0,    01,   0243,
  '2',  '0',  '0',  '0',  017,  06,   03,   'U',  035,  021,  04,   010,  '0',  06,   0207, 04,
  0177, 0,    0,    01,   '0',  035,  06,   03,   'U',  035,  016,  04,   026,  04,   024,  '0',
  'z',  0213, 'C',  'E',  0262, 0351, 'J',  '5',  0355, 0340, 0267, 024,  033,  0263, 0241, 0346,
  037,  0273, 026,  '0',  015,  06,   011,  '*',  0206, 'H',  0206, 0367, 015,  01,   01,   013,
  05,   0,    03,   0202, 01,   01,   0,    'Z',  'X',  'l',  0364, '%',  0376, 025,  'H',  0343,
  'z',  'q',  0275, 'l',  030,  0337, 'H',  'w',  0274, 0317, 0227, 0322, 'j',  0367, 'p',  'u',
  0344, 0345, 'm',  0321, 0342, 0222, ':',  016,  0206, 'R',  027,  0220, 'p',  0330, 'A',  0247,
  0265, 017,  'i',  0361, 06,   0326, '4',  0357, 'A',  'G',  'v',  'b',  '`',  0361, 0300, 0352,
  'U',  '-',  '$',  '<',  0271, 0344, '?',  'W',  0221, 'N',  01,   037,  013,  0201, 0233, 0207,
  'q',  0251, '!',  'k',  0240, 03,   0246, 0305, 0326, 013,  'g',  0355, 0214, 0272, '%',  'S',
  0304, 0307, 0326, '[',  'N',  'd',  0214, 030,  0250, 0134, 037,  'A',  'u',  0256, 0243, 0330,
  0360, 0226, 0307, '8',  0251, 0376, '_',  0206, 0364, '~',  'I',  'O',  0205, 030,  '^',  035,
  0302, 0,    '/',  'u',  011,  0134, 'x',  't',  011,  ' ',  020,  0234, 012,  0325, ' ',  0203,
  0375, 013,  'P',  'b',  0214, 0301, 'X',  0232, 0317, 036,  0367, 'y',  '/',  0340, 0315, 013,
  032,  't',  0263, 0215, 0262, 0326, 0253, 0223, '#',  034,  0351, 0232, 0361, 0352, 0224, 0240,
  '/',  0250, 033,  0226, 0255, 0325, 0363, 0262, 'N',  0205, '*',  '4',  '#',  0306, 0351, 0353,
  0264, 0264, 0211, 0335, '!',  0254, 0351, '6',  033,  0225, '=',  037,  021,  0205, 'F',  '~',
  0202, 'T',  '~',  0367, 0246, 0236, 022,  0314, '`',  'n',  's',  0365, 0271, 0366, 0223, 0223,
  0316, 0373, 0315, '2',  06,   0224, '5',  0357, 011,  'i',  010,  0234, 0272, 020,  0374, 'H',
  'h',  031,  0316, 0227, 0364, '<',  '5',  'p',  0264, 0232, 0134, 0204, 0217, 'e',  'V',  01,
  0205, 0337, 0361, 0217, 'X',  0275, 0371, 0213, 0255, 01,   0230, 0371, 0245, 0312, 'M',  0256,
  0363, 0274, 0356, '|',  '$',  '*',  0316, 0242, 0276, 0311, 0217, 012,
};

class SslCertificates {
public:
  static const char* cassandra_pem() {
    return "-----BEGIN CERTIFICATE-----\12"
           "MIIDtDCCApygAwIBAgIEHJOKVzANBgkqhkiG9w0BAQsFADCBgDELMAkGA1UEBhMCVVMxEzARBgNV\12"
           "BAgTCkNhbGlmb3JuaWExFDASBgNVBAcTC1NhbnRhIENsYXJhMRYwFAYDVQQKEw1EYXRhU3RheCBJ\12"
           "bmMuMRowGAYDVQQLExFEcml2ZXJzIGFuZCBUb29sczESMBAGA1UEAxMJMTI3LjAuMC4xMCAXDTE0\12"
           "MTEwNjIxMDcyNVoYDzIxMTQxMDEzMjEwNzI1WjCBgDELMAkGA1UEBhMCVVMxEzARBgNVBAgTCkNh\12"
           "bGlmb3JuaWExFDASBgNVBAcTC1NhbnRhIENsYXJhMRYwFAYDVQQKEw1EYXRhU3RheCBJbmMuMRow\12"
           "GAYDVQQLExFEcml2ZXJzIGFuZCBUb29sczESMBAGA1UEAxMJMTI3LjAuMC4xMIIBIjANBgkqhkiG\12"
           "9w0BAQEFAAOCAQ8AMIIBCgKCAQEA17rsAAH3Y4nVdDYUfuSMKexzuOI7Zyj4R8f5QyUwOyC07X1z\12"
           "pVKnKHrT3aWk1tRla5SouLO4WhSmoHdRL1MIjxf0vfprXI4ykqUpxLVWL1INJZ8taVxF/xdqNUyC\12"
           "OD3J0Z8rt4qMg1FJ5K6MetZs4+tEOENM1QQB0p+JT3uR6OGlr54kxPHc4dMv4OxMVGHQt9Rz92Lt\12"
           "su7WXl+tpVR3M4jC4eh2V2mTW1zT24jtb9CtKculmQ3lm3UcKPeYZ3nZvkPbVrLznbVu+owg/wHF\12"
           "H71h7qg1qKnX6Pg3axBF3SHpPnO+7JFdPLiLjHUO8Tgveo/bfp50M4cHTAsAHIUUawIDAQABozIw\12"
           "MDAPBgNVHREECDAGhwR/AAABMB0GA1UdDgQWBBSYgUm5u85lVKpbpcfEXCShxJOSRTANBgkqhkiG\12"
           "9w0BAQsFAAOCAQEAxpPDnB87k3YngcXHlgvuigErB3VtTV9RPdCQBiyWX+DtYkdKS7eVyM0lXNxq\12"
           "XqulAglZLCY6yKajIUF+LvWihNORWONWgAlo5EWYT+tzS+dKsuIhpgEM98mL1bgzqBwNaVOHla20\12"
           "BjZTVrErg2Ve7EYyQswE/DVa1ivnhIWM25v02DcT8n+fUd5F/pprLydGji0ElnNDNAeBLH7g07fY\12"
           "up+4nu8jCYZKa7iQEPIJkruJyYNrfkjbmXGkI6ahNxO1NO+U8QG5pvzTHq06IU9pU80EDaTEsUYg\12"
           "bHoyn7PBRKGz3g+Pz2x2r3reZK8BLdvvnML3V/dO+tZ4mY3CSl8rVQ==\12"
           "-----END CERTIFICATE-----\12";
  }
  static const char* driver_pem() {
    return "-----BEGIN CERTIFICATE-----\12"
           "MIIDtDCCApygAwIBAgIEGbufkzANBgkqhkiG9w0BAQsFADCBgDELMAkGA1UEBhMCVVMxEzARBgNV\12"
           "BAgTCkNhbGlmb3JuaWExFDASBgNVBAcTC1NhbnRhIENsYXJhMRYwFAYDVQQKEw1EYXRhU3RheCBJ\12"
           "bmMuMRowGAYDVQQLExFEcml2ZXJzIGFuZCBUb29sczESMBAGA1UEAxMJMTI3LjAuMC4xMCAXDTE0\12"
           "MTEwNjIxMDcyNloYDzIxMTQxMDEzMjEwNzI2WjCBgDELMAkGA1UEBhMCVVMxEzARBgNVBAgTCkNh\12"
           "bGlmb3JuaWExFDASBgNVBAcTC1NhbnRhIENsYXJhMRYwFAYDVQQKEw1EYXRhU3RheCBJbmMuMRow\12"
           "GAYDVQQLExFEcml2ZXJzIGFuZCBUb29sczESMBAGA1UEAxMJMTI3LjAuMC4xMIIBIjANBgkqhkiG\12"
           "9w0BAQEFAAOCAQ8AMIIBCgKCAQEAv893jizDENNWt/1bVCOGB5usfEhJdJ33fNJDAHcfUSRxBueS\12"
           "EMFw8lZqItAUnqnZ6KPsh9AfBSL+tz9p6mDxFWFXJC9bAIPpXkXDwbWpy+TQEXJGFnyqtttP9u4D\12"
           "P2mPIFi5iMaai0qVBfnDA/Vu9mQ4p6MD57ETf/mXsHFV0+L/dkN5e+Jsw14knSWvSd380/x4Dlsf\12"
           "rAzzFWrf9S6S5+awuwiIfP8xy51VWuHIkBQfmWRTrlYwq0ZNDhGTDCgeknQ/M/SjMwg7GK43DGdu\12"
           "IkD4fHBI1Hmrn1Jzz63a1O/7YaOEc7ORCA0moIZpiZyPCETvmXvo4oqi19I2iuogDwIDAQABozIw\12"
           "MDAPBgNVHREECDAGhwR/AAABMB0GA1UdDgQWBBQweotDRbLpSjXt4LcUG7Oh5h+7FjANBgkqhkiG\12"
           "9w0BAQsFAAOCAQEAWlhs9CX+FUjjenG9bBjfSHe8z5fSavdwdeTlbdHikjoOhlIXkHDYQae1D2nx\12"
           "BtY070FHdmJg8cDqVS0kPLnkP1eRTgEfC4Gbh3GpIWugA6bF1gtn7Yy6JVPEx9ZbTmSMGKhcH0F1\12"
           "rqPY8JbHOKn+X4b0fklPhRheHcIAL3UJXHh0CSAQnArVIIP9C1BijMFYms8e93kv4M0LGnSzjbLW\12"
           "q5MjHOma8eqUoC+oG5at1fOyToUqNCPG6eu0tIndIazpNhuVPR8RhUZ+glR+96aeEsxgbnP1ufaT\12"
           "k877zTIGlDXvCWkInLoQ/EhoGc6X9Dw1cLSaXISPZVYBhd/xj1i9+Q==\12"
           "-----END CERTIFICATE-----\12";
  }
  static const char* driver_private_pem() {
    return "-----BEGIN RSA PRIVATE KEY-----\12"
           "Proc-Type: 4,ENCRYPTED\12"
           "DEK-Info: AES-128-CBC,07B2EF76F08F6003153F7CE9CB7189AD\12"
           "\12"
           "ryRg2/wWRw26pTYtuV9OZwsePj7O3bDXgElpyeByTc/XEWz4Pq3hc5zl2ioe2JeL\12"
           "ImOsnqRpBaasybSeD5pfat6On7EXYpw72jixklQk8TTiBUSaGzXVkm3QPkFm8prC\12"
           "52X9XDWqj0EzkI1gdK/BWLIU2spiIlrWlLq5r5QDFi2uvK1w2bxlKs/g8BZsOUMg\12"
           "vNk55hd9RonUu0egyUaynSm5LhaK6h0l8+afiaJEKyMvR8fHea/qdleDu8kKjad2\12"
           "81zy5rv5CXRogZbrAu7LWFmYOXzD8G/SGH0jtLVcEzZxy/krEFbrm4tDrcWbTPZI\12"
           "fqXwtbx7zBzcVL5V9bH6sMbaEVdkHMLrHwBsGJt38qZcYal/MtRh4ovnfNTGhiNh\12"
           "oX9ceBsD8J3Wgd6GUa78y1gGziO9u1PUOqnH/4mv8jtdeoH1fGXW91jBy/2czWi2\12"
           "ACOOW4GhljKPIGyPdC4b2EkVYpmLSIJC3173zBAukzFDTf/ik3NMv+fWilcIDl+L\12"
           "VvoUKa+xwH9bBAlaSBax93EeMCI2LrNKkgrpAXnPeaPSliXGFj+3dbNFZ8ievSiq\12"
           "fpZ2tqaBa66lwRllty7SbW5jdMUKzQX3zcezwyVOyc+FkPO6j/MwKevAuTl/iSKx\12"
           "LpYrOQ3USi+e0gDY7HGVBazwK/aakkK+Nbl91gC+iufo5G+/EpesFs2qGTSUWynt\12"
           "UG+BeJo6miVbMMOvl9/rMPCHOu88ecXr5QSmQI5vceb4RyORRd/ogB8/M+0KQu38\12"
           "3IVS7kuuQKoR4s2Q4TFF0t8MHQ9XA7m2BzUVm1C958ET+rOYfsKUPZTJwVCmllTV\12"
           "fuC53gr1SH1rHgD8w/Rv8Th59NI/TJYIzUpofoRhJP9A9ryhJY9zvX0X2EoDI1OJ\12"
           "J2rB8ZTLbA6/nWDmCANC+Wa9LP3ArKo+bi0Vdzjrt/1jyhThd9wG85y0WdO108mr\12"
           "7LYhPhKAPuvvfY6X54GRZOI3vU3UkR8A8KFfjbQAPvRSnhThLbEJLqb+bq0azSnz\12"
           "qol1K6FDhHCPOY+emoQyAYXINOK2RqzSjFc3mL4nVI/9L4tn7O67OFtEt4eZl3kK\12"
           "EqWRH48FPlBdyRoR572Z5u0cKqcpDCWdxIP7Yo99+AgoCHtzHGolBJzxEmJXUfEJ\12"
           "T4RQ3ufXZicOhIn7JF3+Q1ZxHueazYeYAcI179fyZ1liDyY+rfLcwO/8/xtO1Rde\12"
           "nNLbCpMyflJGoiayDbpXUaqJ02Ag5MG0dsocYjK8g1IlRpYx12MqdRHK2bGIOoNR\12"
           "PedHkYYoOCKzM8YGD9JeadAl2ljcrChoL4anacbm5j4IQ+goL3k5BwITXwFR7jnD\12"
           "LS3dvoUAgtbs+hiLEsQ/o2jLhG0MzF/TAwJH2Lk7vMKpxrTwtmFjJMr04rq6hmIX\12"
           "o/pDyWSlKCrf9SSJM9trJ1pWQZZLjBRCSRwMU5Z6+yIntAuIyQyQ+s5Ny0mZqd5v\12"
           "yS+9SG411TWou2l+Kl+O1Uk+HiIvMF6konjqLWGkdBh6xGUo1ZtUvT8T0NZ+bCnR\12"
           "HKYjq/buZugYcAxbYGAxuQSTpJouOEiW8hQGG37cWgjBGmuQq5PNbWO68X8CPG6h\12"
           "-----END RSA PRIVATE KEY-----\12";
  }
  static const char* cassandra_invalid_pem() {
    return "-----BEGIN CERTIFICATE-----\12"
           "MIIDjTCCAnWgAwIBAgIEaDbI+DANBgkqhkiG9w0BAQsFADBsMRAwDgYDVQQGEwdJTlZBTElEMRAw\12"
           "DgYDVQQIEwdJTlZBTElEMRAwDgYDVQQHEwdJTlZBTElEMRAwDgYDVQQKEwdJTlZBTElEMRAwDgYD\12"
           "VQQLEwdJTlZBTElEMRAwDgYDVQQDEwdJTlZBTElEMCAXDTE0MTEwNjIxMTA0MFoYDzIxMTQxMDEz\12"
           "MjExMDQwWjBsMRAwDgYDVQQGEwdJTlZBTElEMRAwDgYDVQQIEwdJTlZBTElEMRAwDgYDVQQHEwdJ\12"
           "TlZBTElEMRAwDgYDVQQKEwdJTlZBTElEMRAwDgYDVQQLEwdJTlZBTElEMRAwDgYDVQQDEwdJTlZB\12"
           "TElEMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhJ1PIFcIQ5ae8vKmR9C259JSJLR1\12"
           "uzHA7vVnMxI01t/D9oyGT4uZTQdulB+CAdiRDoiYVHtAyiWKlTPHJYOhFEv+eUWtpEkrP/CVG1sz\12"
           "tyT4Gu7fyI2EuGkn7UExkudUs4bhJ3hXGqNzK8mohlXphOu6pGucRJD/EhGfUb8g4dLpnRYSz4ej\12"
           "v0X05HGRJT9iQDjahw2OPfp7kcuA1+qjW0hQLC/9IbhvS2Ht/FolC1eHiVf+6w7/l+JzKyv2iAjV\12"
           "fx4r7zawUcoShuCdChaVRRGg7MOn6gcnFhl7j3EeOZy99WCTZRR6Vu69UKL5muIAGfSbd1kgeZSt\12"
           "5R3tyHu+xQIDAQABozUwMzASBgNVHREECzAJggdJTlZBTElEMB0GA1UdDgQWBBQXzNgc4d/tmfCG\12"
           "oQwcntCTgAyQqjANBgkqhkiG9w0BAQsFAAOCAQEAPoImWo5R8i1R3UXOnHu8ZczgBKsGmEbzxk2l\12"
           "3xe8a37uUtRyWt/rQlpTdT4Q8J7/W4yBQqdKkkjh6YCiTHysN/RrwwXLw7qRqew+vmALIjI916Z5\12"
           "Bzi35rZ4lTZ1Dx+Dnw/kaXzRyesKt2kb9gYD2GRvxLFoJ64I6MioqUhk3rKkNtRep4uXIkB+uz0K\12"
           "A9UxF3Hd0GeWw3lYkX46jhbK5EnVYc6CCtXyjUA0VBjjCTTfCP1nSc+/VWGFW1LMcw8bsjWX7xyW\12"
           "DCqT/N2ui77Ea4uCAaDkRwhTXxYQlSQLnT//43SEoOLoqI7x8hWht1E/0Dfhz68J+HU98tAY6Mk7\12"
           "sQ==\12-----END CERTIFICATE-----\12";
  }
  static const char* driver_invalid_pem() {
    return "-----BEGIN CERTIFICATE-----\12"
           "MIID6jCCAtKgAwIBAgIEBshxwjANBgkqhkiG9w0BAQsFADCBljEXMBUGA1UEBhMORFJJVkVSLUlO\12"
           "VkFMSUQxFzAVBgNVBAgTDkRSSVZFUi1JTlZBTElEMRcwFQYDVQQHEw5EUklWRVItSU5WQUxJRDEX\12"
           "MBUGA1UEChMORFJJVkVSLUlOVkFMSUQxFzAVBgNVBAsTDkRSSVZFUi1JTlZBTElEMRcwFQYDVQQD\12"
           "Ew5EUklWRVItSU5WQUxJRDAgFw0xNDExMDYyMTEwNDFaGA8yMTE0MTAxMzIxMTA0MVowgZYxFzAV\12"
           "BgNVBAYTDkRSSVZFUi1JTlZBTElEMRcwFQYDVQQIEw5EUklWRVItSU5WQUxJRDEXMBUGA1UEBxMO\12"
           "RFJJVkVSLUlOVkFMSUQxFzAVBgNVBAoTDkRSSVZFUi1JTlZBTElEMRcwFQYDVQQLEw5EUklWRVIt\12"
           "SU5WQUxJRDEXMBUGA1UEAxMORFJJVkVSLUlOVkFMSUQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw\12"
           "ggEKAoIBAQCp8z0FEEFDNJUbT5L4TIUGGWzqaMz8iQVP7p4vjPsUmQX6OGjnnZlOoKfhr+S8/pZm\12"
           "7B70z9pExZyeKaOzsmYMV3042/jVb+AL1nPki4lhmei6SHjAYvr1VNInPwbA/mx3K0ZCuJrJVhxJ\12"
           "zGoWO+RzoWg3IG6cfNOxMXUzTEJ8z7nJWoHbmFu3aXNuPjRygSV6vPgFCSLjN4wLUq5jdxcP1dXl\12"
           "FDk0JQIG1Z1WVKrmg19CDMD53QscHiXCuFiPcBKZgaTJujZsvk9NtWYZzVIb4+itIR9mol1+IKzp\12"
           "JtPyqyV8VJPcmB1VVS/EJIzaJSnv0lE39tlEXTzgs1Ym9O17AgMBAAGjPDA6MBkGA1UdEQQSMBCC\12"
           "DkRSSVZFUi1JTlZBTElEMB0GA1UdDgQWBBRat7P/qYqIRx1QMGfRUSD0IEQXYTANBgkqhkiG9w0B\12"
           "AQsFAAOCAQEAPGXXJlmszaLvtdtwb63tniyO1/44dMVkRlSkZbcn0evjmxN1Jrf7l7C41jTfLlU4\12"
           "dQ+USKxd+Sx2pNWmQi9NmH8vK/ozhF+M0qWTF/IiZHKShEcsYoSDUN+q9fqUVK1ABtzE9qKb9nn4\12"
           "Ts0ZUKvFXEUKzsFmf8tZYRFn7NFntXuFxDMVxrYkyRAlwJE31pZ5slZvwTSgik6OYnEd38BfuAHb\12"
           "OVRCz+y7NMcfwKD+DsKDwAgw6cJ8uprSHf1LkJ0KcXVlcapSynuEJVdwXKQqR+e5/CXUi/6+QY0M\12"
           "cAHazCFHOKxSQ/G7n+8xDx3r6jHxyE956u5jf5FRqUbaVIBMdg==\12"
           "-----END CERTIFICATE-----\12";
  }
  static const char* driver_private_invalid_pem() {
    return "-----BEGIN CERTIFICATE-----\12"
           "MIID6jCCAtKgAwIBAgIEBshxwjANBgkqhkiG9w0BAQsFADCBljEXMBUGA1UEBhMORFJJVkVSLUlO\12"
           "VkFMSUQxFzAVBgNVBAgTDkRSSVZFUi1JTlZBTElEMRcwFQYDVQQHEw5EUklWRVItSU5WQUxJRDEX\12"
           "MBUGA1UEChMORFJJVkVSLUlOVkFMSUQxFzAVBgNVBAsTDkRSSVZFUi1JTlZBTElEMRcwFQYDVQQD\12"
           "Ew5EUklWRVItSU5WQUxJRDAgFw0xNDExMDYyMTEwNDFaGA8yMTE0MTAxMzIxMTA0MVowgZYxFzAV\12"
           "BgNVBAYTDkRSSVZFUi1JTlZBTElEMRcwFQYDVQQIEw5EUklWRVItSU5WQUxJRDEXMBUGA1UEBxMO\12"
           "RFJJVkVSLUlOVkFMSUQxFzAVBgNVBAoTDkRSSVZFUi1JTlZBTElEMRcwFQYDVQQLEw5EUklWRVIt\12"
           "SU5WQUxJRDEXMBUGA1UEAxMORFJJVkVSLUlOVkFMSUQwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw\12"
           "ggEKAoIBAQCp8z0FEEFDNJUbT5L4TIUGGWzqaMz8iQVP7p4vjPsUmQX6OGjnnZlOoKfhr+S8/pZm\12"
           "7B70z9pExZyeKaOzsmYMV3042/jVb+AL1nPki4lhmei6SHjAYvr1VNInPwbA/mx3K0ZCuJrJVhxJ\12"
           "zGoWO+RzoWg3IG6cfNOxMXUzTEJ8z7nJWoHbmFu3aXNuPjRygSV6vPgFCSLjN4wLUq5jdxcP1dXl\12"
           "FDk0JQIG1Z1WVKrmg19CDMD53QscHiXCuFiPcBKZgaTJujZsvk9NtWYZzVIb4+itIR9mol1+IKzp\12"
           "JtPyqyV8VJPcmB1VVS/EJIzaJSnv0lE39tlEXTzgs1Ym9O17AgMBAAGjPDA6MBkGA1UdEQQSMBCC\12"
           "DkRSSVZFUi1JTlZBTElEMB0GA1UdDgQWBBRat7P/qYqIRx1QMGfRUSD0IEQXYTANBgkqhkiG9w0B\12"
           "AQsFAAOCAQEAPGXXJlmszaLvtdtwb63tniyO1/44dMVkRlSkZbcn0evjmxN1Jrf7l7C41jTfLlU4\12"
           "dQ+USKxd+Sx2pNWmQi9NmH8vK/ozhF+M0qWTF/IiZHKShEcsYoSDUN+q9fqUVK1ABtzE9qKb9nn4\12"
           "Ts0ZUKvFXEUKzsFmf8tZYRFn7NFntXuFxDMVxrYkyRAlwJE31pZ5slZvwTSgik6OYnEd38BfuAHb\12"
           "OVRCz+y7NMcfwKD+DsKDwAgw6cJ8uprSHf1LkJ0KcXVlcapSynuEJVdwXKQqR+e5/CXUi/6+QY0M\12"
           "cAHazCFHOKxSQ/G7n+8xDx3r6jHxyE956u5jf5FRqUbaVIBMdg==\12"
           "-----END CERTIFICATE-----\12";
  }
  static const char* multi_cert_pem() {
    std::string combo = driver_invalid_pem();
    combo.append("\n");
    combo.append(cassandra_pem());
    return combo.c_str();
  }

  static const char* driver_private_pem_password() { return "driver"; }

  static void write_ccm_server_files() {
    std::string ssl_path = Utils::temp_directory() + Utils::PATH_SEPARATOR + "ssl";
    Utils::mkdir(ssl_path);

    {
      std::string file = ssl_path + Utils::PATH_SEPARATOR + "cassandra.crt";
      std::ofstream out(file.c_str(), std::ios::binary | std::ios::trunc);
      out.write(reinterpret_cast<char const*>(cassandra_crt), sizeof(cassandra_crt));
    }
    {
      std::string file = ssl_path + Utils::PATH_SEPARATOR + "keystore.jks";
      std::ofstream out(file.c_str(), std::ios::binary | std::ios::trunc);
      out.write(reinterpret_cast<char const*>(keystore_jks), sizeof(keystore_jks));
    }
    {
      std::string file = ssl_path + Utils::PATH_SEPARATOR + "truststore.jks";
      std::ofstream out(file.c_str(), std::ios::binary | std::ios::trunc);
      out.write(reinterpret_cast<char const*>(truststore_jks), sizeof(truststore_jks));
    }
  }
};

} // namespace test

#endif
