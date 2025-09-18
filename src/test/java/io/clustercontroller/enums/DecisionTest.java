package io.clustercontroller.enums;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class DecisionTest {
    
    @Test
    void testMergeLogic() {
        // NO overrides everything
        assertThat(Decision.YES.merge(Decision.NO)).isEqualTo(Decision.NO);
        assertThat(Decision.NO.merge(Decision.YES)).isEqualTo(Decision.NO);
        assertThat(Decision.THROTTLE.merge(Decision.NO)).isEqualTo(Decision.NO);
        
        // THROTTLE overrides YES
        assertThat(Decision.YES.merge(Decision.THROTTLE)).isEqualTo(Decision.THROTTLE);
        assertThat(Decision.THROTTLE.merge(Decision.YES)).isEqualTo(Decision.THROTTLE);
        
        // YES with YES stays YES
        assertThat(Decision.YES.merge(Decision.YES)).isEqualTo(Decision.YES);
    }
}
