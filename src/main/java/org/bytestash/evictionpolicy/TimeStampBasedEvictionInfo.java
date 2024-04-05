package org.bytestash.evictionpolicy;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Timestamp;
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TimeStampBasedEvictionInfo extends EvictionInfo {
        Timestamp oldestTimeStamp;
}
