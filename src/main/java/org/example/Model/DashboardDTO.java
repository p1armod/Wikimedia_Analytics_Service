package org.example.Model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DashboardDTO {
    private CountPair humanVsBot;

    private CountPair MajorVsMinor;

    private Map<String,Long> editSizes;

    private Map<String, Long> editTypes;

    private Map<String,Long> wikiMetrics;

    private DelayMetricDTO delayMetrics;
}
