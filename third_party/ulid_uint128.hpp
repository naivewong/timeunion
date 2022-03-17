#pragma once

#include <boost/functional/hash.hpp>
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
typedef __uint128_t ULID;

/* Added by Alec */
struct ULIDHasher {
  std::size_t operator()(const ULID &t) const {
    // Start with a hash value of 0    .
    std::size_t seed = 0;

    // Modify 'seed' by XORing and bit-shifting in
    // one member of 'Key' after the other:
    boost::hash_combine(seed, boost::hash_value(t));

    // Return the result.
    return seed;
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
 * timestamp:
 * dst[0]: first 3 bits of data[0]
 * dst[1]: last 5 bits of data[0]
 * dst[2]: first 5 bits of data[1]
 * dst[3]: last 3 bits of data[1] + first 2 bits of data[2]
 * dst[4]: bits 3-7 of data[2]
 * dst[5]: last bit of data[2] + first 4 bits of data[3]
 * dst[6]: last 4 bits of data[3] + first bit of data[4]
 * dst[7]: bits 2-6 of data[4]
 * dst[8]: last 2 bits of data[4] + first 3 bits of data[5]
 * dst[9]: last 5 bits of data[5]
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
