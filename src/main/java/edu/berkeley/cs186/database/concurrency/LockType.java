package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if (a == NL || b == NL) {
            return true;
        }
        // TODO(proj4_part1): implement
        if (a == S && b == S) {
            return true;
        }
        if (a == IS && b == IS) {
            return true;
        }
        if ((a == IS && b == IX) || (b == IS && a == IX)) {
            return true;
        }
        if ((a == IS && b == S) || (b == IS && a == S)) {
            return true;
        }
        if ((a == IS && b == SIX) || (b == IS && a == SIX)) {
            return true;
        }
        if (a == IX && b == IX) {
            return true;
        }


        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if (childLockType == NL) {
            return true;
        }
        if (parentLockType == IS && childLockType == S) {
            return true;
        }
        if (parentLockType == IX && childLockType == S) {
            return true;
        }
        if (parentLockType == IX && childLockType == IS) {
            return true;
        }
        if (parentLockType == IS && childLockType == IS) {
            return  true;
        }
        if (parentLockType == IX && childLockType == IX) {
            return true;
        }
        if (parentLockType == IX && childLockType == X) {
            return true;
        }
        if (parentLockType == SIX && childLockType == X) {
            return true;
        }
        if (parentLockType == SIX && childLockType == IX) {
            return true;
        }

        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        if (substitute == required) {
            return true;
        }
        // TODO(proj4_part1): implement
        if (required == NL) {
            return true;
        }
        if (substitute == X && required != X) {
            return true;
        }

        if (substitute == IX && required == IS) {
            return true;
        }
        if (substitute == SIX && required == IS) {
            return true;
        }
        if (substitute == SIX && required == S) {
            return true;
        }
        if (substitute == SIX && required == IX) {
            return true;
        }
        if (substitute == S && required == IS) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

