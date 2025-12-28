package org.example.Service;


import lombok.RequiredArgsConstructor;
import org.example.Model.EditCountEvent;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RealtimeUpdatesService {

    private final SimpMessagingTemplate messagingTemplate;

    public void publish(EditCountEvent event){
        messagingTemplate.convertAndSend("/topic/edits", event);
    }
}
