package org.example.Model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EditCountEvent {
    private String user;
    private long count;
    private long windowStart;
}
