# Orch8 Mobile SDK — consumer ProGuard rules.
# These rules are automatically included when this AAR is consumed by an app.

# Preserve JNA classes used by UniFFI-generated bindings.
-keep class com.sun.jna.** { *; }
-keep class * implements com.sun.jna.** { *; }

# Preserve the UniFFI-generated Kotlin classes.
-keep class io.orch8.mobile.** { *; }

# Preserve native method signatures.
-keepclasseswithmembernames class * {
    native <methods>;
}
