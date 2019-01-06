package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.decorator.AlertMessageDecorator;
import com.ctrip.xpipe.redis.console.alert.decorator.Decorator;
import com.ctrip.xpipe.redis.console.alert.decorator.RecoverMessageDecorator;
import com.ctrip.xpipe.redis.console.alert.decorator.GroupedAlertMessageDecorator;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class DecoratorManager {

    @Autowired
    Map<String, Decorator> decorators;

    /* There's another option to generate content for recover message
    So the @param isAlertMessage is the pivot to switch between two phases
    */
    public Pair<String, String> generateTitleAndContent(AlertEntity alert, boolean isAlertMessage) {
        Decorator decorator = getDecorator(isAlertMessage);
        String content = decorator.generateContent(alert);
        String title = decorator.generateTitle(alert);
        return new Pair<>(title, content);
    }

    private Decorator getDecorator(boolean isAlertMessage) {
        if(isAlertMessage) {
            return decorators.get(AlertMessageDecorator.ID);
        } else {
            return decorators.get(RecoverMessageDecorator.ID);
        }
    }

    public Pair<String, String> generateTitleAndContent(Map<ALERT_TYPE, Set<AlertEntity>> alerts, boolean isAlertMessage) {
        GroupedAlertMessageDecorator decorator;
        if(isAlertMessage) {
            decorator = (GroupedAlertMessageDecorator) decorators.get(GroupedAlertMessageDecorator.ID);
        } else {
            decorator = (GroupedAlertMessageDecorator) decorators.get(RecoverMessageDecorator.ID);
        }
        String content = decorator.generateBody(alerts);
        String title = decorator.generateTitle();
        return new Pair<>(title, content);
    }


}
