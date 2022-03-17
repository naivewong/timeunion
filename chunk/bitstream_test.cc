#include "chunk/BitStream.hpp"

#include "base/Endian.hpp"
#include "chunk/XORAppender.hpp"
#include "chunk/XORChunk.hpp"
#include "chunk/XORIterator.hpp"
#include "gtest/gtest.h"

namespace tsdb {
namespace chunk {

class BitStreamTest : public testing::Test {};

TEST_F(BitStreamTest, Test1) {
  uint8_t buf[256];
  BitStreamV2 b(buf);
  b.write_bit(1);
  b.write_byte(28);
  b.write_bit(0);
  b.write_bits(284617, 32);
  uint8_t temp[10];
  int encoded = base::encode_signed_varint(temp, -777);
  for (int i = 0; i < encoded; i++) b.write_byte(temp[i]);

  ASSERT_EQ(b.read_bit(0), 1);
  ASSERT_EQ(b.read_byte(1), 28);
  ASSERT_EQ(b.read_bit(9), 0);
  ASSERT_EQ(b.read_bits(10, 32), 284617);
  ASSERT_EQ(b.read_signed_varint(42).first, -777);
}

TEST_F(BitStreamTest, Test2) {
  uint8_t buf[256];
  XORChunkV2 c(buf);
  auto app = c.appender();

  app->append(10, 0.1);
  app->append(20, 0.2);
  app->append(30, 0.3);
  app->append(40, 0.4);
  app->append(50, 0.5);
  app->append(60, 0.6);

  std::cout << c.size() << std::endl;
  std::cout << base::get_uint16_big_endian(buf) << std::endl;

  auto it = c.xor_iterator();
  ASSERT_TRUE(it->next());
  ASSERT_EQ(10, it->at().first);
  ASSERT_EQ(0.1, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(20, it->at().first);
  ASSERT_EQ(0.2, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(30, it->at().first);
  ASSERT_EQ(0.3, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(40, it->at().first);
  ASSERT_EQ(0.4, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(50, it->at().first);
  ASSERT_EQ(0.5, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(60, it->at().first);
  ASSERT_EQ(0.6, it->at().second);

  ASSERT_FALSE(it->next());
}

TEST_F(BitStreamTest, Test3) {
  uint8_t buf1[256], buf2[256], buf3[256];
  NullSupportedXORGroupChunkV2 c({0, 1}, buf1, {buf2, buf3});
  auto app = c.appender();

  app->append(10, {0.1, 0.1});
  app->append(20, {0.2, 0.2});
  app->append(30, {0.3, 0.3});
  app->append(40, {0.4, 0.4});
  app->append(50, {0.5, 0.5});
  app->append(60, {0.6, 0.6});

  std::cout << c.size() << std::endl;
  std::cout << base::get_uint16_big_endian(buf1) << std::endl;

  auto it = c.iterator(0);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(10, it->at().first);
  ASSERT_EQ(0.1, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(20, it->at().first);
  ASSERT_EQ(0.2, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(30, it->at().first);
  ASSERT_EQ(0.3, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(40, it->at().first);
  ASSERT_EQ(0.4, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(50, it->at().first);
  ASSERT_EQ(0.5, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(60, it->at().first);
  ASSERT_EQ(0.6, it->at().second);

  it = c.iterator(1);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(10, it->at().first);
  ASSERT_EQ(0.1, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(20, it->at().first);
  ASSERT_EQ(0.2, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(30, it->at().first);
  ASSERT_EQ(0.3, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(40, it->at().first);
  ASSERT_EQ(0.4, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(50, it->at().first);
  ASSERT_EQ(0.5, it->at().second);
  ASSERT_TRUE(it->next());
  ASSERT_EQ(60, it->at().first);
  ASSERT_EQ(0.6, it->at().second);

  ASSERT_FALSE(it->next());
}

}  // namespace chunk
}  // namespace tsdb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}