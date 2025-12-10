package io.clustercontroller.enums;

/**
 * Allocation decision result.
 * 
 * Merge precedence: NO > THROTTLE > YES
 */
public enum Decision {
    YES, NO, THROTTLE;
    
    public Decision merge(Decision other) {
        if (this == NO || other == NO) return NO;
        if (this == THROTTLE || other == THROTTLE) return THROTTLE;
        return YES;
    }
}