#pragma once

#include <chrono>
#include <cstdlib>
#include <ctime>
#include <functional>
#include <random>
#include <vector>

namespace ulid {

/**
 * ULID is a 16 byte Universally Unique Lexicographically Sortable Identifier
 * */
struct ULID {
  uint8_t data[16];

  ULID() {
    // for (int i = 0 ; i < 16 ; i++) {
    // 	data[i] = 0;
    // }

    // unrolled loop
    data[0] = 0;
    data[1] = 0;
    data[2] = 0;
    data[3] = 0;
    data[4] = 0;
    data[5] = 0;
    data[6] = 0;
    data[7] = 0;
    data[8] = 0;
    data[9] = 0;
    data[10] = 0;
    data[11] = 0;
    data[12] = 0;
    data[13] = 0;
    data[14] = 0;
    data[15] = 0;
  }

  ULID(uint64_t val) {
    // for (int i = 0 ; i < 16 ; i++) {
    // 	data[15 - i] = static_cast<uint8_t>(val);
    // 	val >>= 8;
    // }

    // unrolled loop
    data[15] = static_cast<uint8_t>(val);

    val >>= 8;
    data[14] = static_cast<uint8_t>(val);

    val >>= 8;
    data[13] = static_cast<uint8_t>(val);

    val >>= 8;
    data[12] = static_cast<uint8_t>(val);

    val >>= 8;
    data[11] = static_cast<uint8_t>(val);

    val >>= 8;
    data[10] = static_cast<uint8_t>(val);

    val >>= 8;
    data[9] = static_cast<uint8_t>(val);

    val >>= 8;
    data[8] = static_cast<uint8_t>(val);

    data[7] = 0;
    data[6] = 0;
    data[5] = 0;
    data[4] = 0;
    data[3] = 0;
    data[2] = 0;
    data[1] = 0;
    data[0] = 0;
  }

  ULID(const ULID &other) {
    // for (int i = 0 ; i < 16 ; i++) {
    // 	data[i] = other.data[i];
    // }

    // unrolled loop
    data[0] = other.data[0];
    data[1] = other.data[1];
    data[2] = other.data[2];
    data[3] = other.data[3];
    data[4] = other.data[4];
    data[5] = other.data[5];
    data[6] = other.data[6];
    data[7] = other.data[7];
    data[8] = other.data[8];
    data[9] = other.data[9];
    data[10] = other.data[10];
    data[11] = other.data[11];
    data[12] = other.data[12];
    data[13] = other.data[13];
    data[14] = other.data[14];
    data[15] = other.data[15];
  }

  ULID &operator=(const ULID &other) {
    // for (int i = 0 ; i < 16 ; i++) {
    // 	data[i] = other.data[i];
    // }

    // unrolled loop
    data[0] = other.data[0];
    data[1] = other.data[1];
    data[2] = other.data[2];
    data[3] = other.data[3];
    data[4] = other.data[4];
    data[5] = other.data[5];
    data[6] = other.data[6];
    data[7] = other.data[7];
    data[8] = other.data[8];
    data[9] = other.data[9];
    data[10] = other.data[10];
    data[11] = other.data[11];
    data[12] = other.data[12];
    data[13] = other.data[13];
    data[14] = other.data[14];
    data[15] = other.data[15];

    return *this;
  }

  ULID(ULID &&other) {
    // for (int i = 0 ; i < 16 ; i++) {
    // 	data[i] = other.data[i];
    // 	other.data[i] = 0;
    // }

    // unrolled loop
    data[0] = other.data[0];
    other.data[0] = 0;

    data[1] = other.data[1];
    other.data[1] = 0;

    data[2] = other.data[2];
    other.data[2] = 0;

    data[3] = other.data[3];
    other.data[3] = 0;

    data[4] = other.data[4];
    other.data[4] = 0;

    data[5] = other.data[5];
    other.data[5] = 0;

    data[6] = other.data[6];
    other.data[6] = 0;

    data[7] = other.data[7];
    other.data[7] = 0;

    data[8] = other.data[8];
    other.data[8] = 0;

    data[9] = other.data[9];
    other.data[9] = 0;

    data[10] = other.data[10];
    other.data[10] = 0;

    data[11] = other.data[11];
    other.data[11] = 0;

    data[12] = other.data[12];
    other.data[12] = 0;

    data[13] = other.data[13];
    other.data[13] = 0;

    data[14] = other.data[14];
    other.data[14] = 0;

    data[15] = other.data[15];
    other.data[15] = 0;
  }

  ULID &operator=(ULID &&other) {
    // for (int i = 0 ; i < 16 ; i++) {
    // 	data[i] = other.data[i];
    // 	other.data[i] = 0;
    // }

    // unrolled loop
    data[0] = other.data[0];
    other.data[0] = 0;

    data[1] = other.data[1];
    other.data[1] = 0;

    data[2] = other.data[2];
    other.data[2] = 0;

    data[3] = other.data[3];
    other.data[3] = 0;

    data[4] = other.data[4];
    other.data[4] = 0;

    data[5] = other.data[5];
    other.data[5] = 0;

    data[6] = other.data[6];
    other.data[6] = 0;

    data[7] = other.data[7];
    other.data[7] = 0;

    data[8] = other.data[8];
    other.data[8] = 0;

    data[9] = other.data[9];
    other.data[9] = 0;

    data[10] = other.data[10];
    other.data[10] = 0;

    data[11] = other.data[11];
    other.data[11] = 0;

    data[12] = other.data[12];
    other.data[12] = 0;

    data[13] = other.data[13];
    other.data[13] = 0;

    data[14] = other.data[14];
    other.data[14] = 0;

    data[15] = other.data[15];
    other.data[15] = 0;

    return *this;
  }
};

/**
 * EncodeTime will encode the first 6 bytes of a uint8_t array to the passed
 * timestamp
 * */
void EncodeTime(time_t timestamp, ULID &ulid);

/**
 * EncodeTimeNow will encode a ULID using the time obtained using
 * std::time(nullptr)
 * */
void EncodeTimeNow(ULID &ulid);

/**
 * EncodeTimeSystemClockNow will encode a ULID using the time obtained using
 * std::chrono::system_clock::now() by taking the timestamp in milliseconds.
 * */
void EncodeTimeSystemClockNow(ULID &ulid);

/**
 * EncodeEntropy will encode the last 10 bytes of the passed uint8_t array with
 * the values generated using the passed random number generator.
 * */
void EncodeEntropy(const std::function<uint8_t()> &rng, ULID &ulid);

/**
 * EncodeEntropyRand will encode a ulid using std::rand
 *
 * std::rand returns values in [0, RAND_MAX]
 * */
void EncodeEntropyRand(ULID &ulid);

extern std::uniform_int_distribution<uint8_t> Distribution_0_255;

/**
 * EncodeEntropyMt19937 will encode a ulid using std::mt19937
 *
 * It also creates a std::uniform_int_distribution to generate values in [0,
 * 255]
 * */
void EncodeEntropyMt19937(std::mt19937 &generator, ULID &ulid);

/**
 * Encode will create an encoded ULID with a timestamp and a generator.
 * */
void Encode(time_t timestamp, const std::function<uint8_t()> &rng, ULID &ulid);

/**
 * EncodeNowRand = EncodeTimeNow + EncodeEntropyRand.
 * */
void EncodeNowRand(ULID &ulid);

/**
 * Create will create a ULID with a timestamp and a generator.
 * */
ULID Create(time_t timestamp, const std::function<uint8_t()> &rng);

/**
 * CreateNowRand:EncodeNowRand = Create:Encode.
 * */
ULID CreateNowRand();

/**
 * Crockford's Base32
 * */
extern const char Encoding[33];

/**
 * MarshalTo will marshal a ULID to the passed character array.
 *
 * Implementation taken directly from oklog/ulid
 * (https://sourcegraph.com/github.com/oklog/ulid@0774f81f6e44af5ce5e91c8d7d76cf710e889ebb/-/blob/ulid.go#L162-190)
 *
 * timestamp:<br>
 * dst[0]: first 3 bits of data[0]<br>
 * dst[1]: last 5 bits of data[0]<br>
 * dst[2]: first 5 bits of data[1]<br>
 * dst[3]: last 3 bits of data[1] + first 2 bits of data[2]<br>
 * dst[4]: bits 3-7 of data[2]<br>
 * dst[5]: last bit of data[2] + first 4 bits of data[3]<br>
 * dst[6]: last 4 bits of data[3] + first bit of data[4]<br>
 * dst[7]: bits 2-6 of data[4]<br>
 * dst[8]: last 2 bits of data[4] + first 3 bits of data[5]<br>
 * dst[9]: last 5 bits of data[5]<br>
 *
 * entropy:
 * follows similarly, except now all components are set to 5 bits.
 * */
void MarshalTo(const ULID &ulid, char dst[26]);

/**
 * Marshal will marshal a ULID to a std::string.
 * */
std::string Marshal(const ULID &ulid);

/**
 * MarshalBinaryTo will Marshal a ULID to the passed byte array
 * */
void MarshalBinaryTo(const ULID &ulid, uint8_t dst[16]);

/**
 * MarshalBinary will Marshal a ULID to a byte vector.
 * */
std::vector<uint8_t> MarshalBinary(const ULID &ulid);

/**
 * dec storesdecimal encodings for characters.
 * 0xFF indicates invalid character.
 * 48-57 are digits.
 * 65-90 are capital alphabets.
 * */
extern const uint8_t dec[256];

/**
 * UnmarshalFrom will unmarshal a ULID from the passed character array.
 * */
void UnmarshalFrom(const char str[26], ULID &ulid);

/**
 * Unmarshal will create a new ULID by unmarshaling the passed string.
 * */
ULID Unmarshal(const std::string &str);

/**
 * UnmarshalBinaryFrom will unmarshal a ULID from the passed byte array.
 * */
void UnmarshalBinaryFrom(const uint8_t b[16], ULID &ulid);

/**
 * Unmarshal will create a new ULID by unmarshaling the passed byte vector.
 * */
ULID UnmarshalBinary(const std::vector<uint8_t> &b);

/**
 * CompareULIDs will compare two ULIDs.
 * returns:
 *     -1 if ulid1 is Lexicographically before ulid2
 *      1 if ulid1 is Lexicographically after ulid2
 *      0 if ulid1 is same as ulid2
 * */
int CompareULIDs(const ULID &ulid1, const ULID &ulid2);

/**
 * Time will extract the timestamp used to generate a ULID
 * */
time_t Time(const ULID &ulid);

// Added by Alec Wang.
bool validate_strict(const std::string &v);

// Added by Alec Wang.
bool validate_weak(const std::string &v);

// Added by Alec Wang.
bool Validate(const std::string &str, bool strict);

};  // namespace ulid
