package org.example.Model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EditCountEvent {
    public String user;
    public long count;
    public long windowStart;
}
