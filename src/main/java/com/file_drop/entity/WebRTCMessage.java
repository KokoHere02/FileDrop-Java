package com.file_drop.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WebRTCMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String type;
    private String from;
    private String to;
    private Object payload;
}
