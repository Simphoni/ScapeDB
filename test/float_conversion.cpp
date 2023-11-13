#include "gtest/gtest.h"

#include <utils/logger.h>

TEST(util, singleToStrTrimmed) {
  EXPECT_STREQ(Logger::singleToStrTrimmed(0.0f), "0.00");
  EXPECT_STREQ(Logger::singleToStrTrimmed(1.0f), "1.00");
  EXPECT_STREQ(Logger::singleToStrTrimmed(697343901.0f), "697344000.00");
  EXPECT_STREQ(Logger::singleToStrTrimmed(69734.3901f), "69734.40");
  EXPECT_STREQ(Logger::singleToStrTrimmed(-1.0f), "-1.00");
  EXPECT_STREQ(Logger::singleToStrTrimmed(-697343901.0f), "-697344000.00");
  EXPECT_STREQ(Logger::singleToStrTrimmed(-69734.3901f), "-69734.40");
}