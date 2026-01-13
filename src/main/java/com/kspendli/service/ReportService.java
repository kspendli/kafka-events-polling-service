package com.kspendli.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ReportService {

    public void processReport(Object event) {
        log.info("successfully processed");
    }
}