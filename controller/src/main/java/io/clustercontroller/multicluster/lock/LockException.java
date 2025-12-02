package io.clustercontroller.multicluster.lock;

/**
 * Exception thrown when lock operations fail.
 */
public class LockException extends Exception {
    
    public LockException(String message) {
        super(message);
    }
    
    public LockException(String message, Throwable cause) {
        super(message, cause);
    }
}
