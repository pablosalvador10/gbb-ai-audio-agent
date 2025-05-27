# Test Scenarios for Healthcare Support Agent

## Part 1 — Routine Assistance

### Scenario 1: User Authentication
**Agent:**  
> Hello from XMYX Healthcare Company! Before I can assist you, let’s verify your identity. How may I address you?

**User:**  
> My name is Alice Brown, I was born on 1987‑04‑12, and my phone number is 555‑297‑1078.

**Action:** `authenticate_user`  
**Result:** Authentication succeeds.  

**Agent:**  
> Thank you, Alice. How can I assist you today?

---

### Scenario 2: New Prescription Entry
**User:**  
> I need a new prescription for Amoxicillin, 500 mg three times a day, filled at City Pharmacy.

**Action:** `fill_new_prescription` (medicine, dosage, pharmacy)  
**Result:** New prescription recorded.  

**Agent:**  
> Your prescription for Amoxicillin (500 mg three times daily) has been added and will be filled at City Pharmacy today. How else can I help?

---

### Scenario 3: Side‑Effects Lookup
**User:**  
> What side effects should I expect when I start taking Amoxicillin, and what about long‑term?

**Action:** `lookup_side_effects`  
**Result:** Initial and long‑term side‑effect lists retrieved.  

**Agent:**  
> Common initial side effects include nausea, diarrhea, and rash. Over longer use, you might see yeast infections or antibiotic resistance. Anything else?

---

### Scenario 4: Drug‑Drug Interaction Check
**User:**  
> Is it safe to take Amoxicillin along with my current medications?

1. **Action:** `get_current_prescriptions` (e.g., Metformin)  
2. **Action:** `check_drug_interactions` (new medication, existing list)  

**Result:** Interaction assessment completed.  

**Agent:**  
> I see you’re on Metformin and Lisinopril. No known interactions with Amoxicillin. You should be safe. Any other questions?

---

### Scenario 5: Appointment Scheduling
**User:**  
> I’d like to schedule a follow‑up consultation for next Tuesday at 10:30 AM.

**Action:** `schedule_appointment`  
**Result:** Appointment successfully scheduled.  

**Agent:**  
> Your follow‑up for next Tuesday at 10:30 AM is booked. Anything else?

---

### Scenario 6: Prescription Refill
**User:**  
> Please refill my Metformin prescription.

**Action:** `refill_prescription`  
**Result:** Refill request submitted.  

**Agent:**  
> Your Metformin refill has been sent to Town Pharmacy. Would you like more information?

---

### Scenario 7: Medication Information
**User:**  
> Tell me about Metformin—its uses and common side effects.

**Action:** `lookup_medication_info`  
**Result:** Usage summary and side‑effects list returned.  

**Agent:**  
> Metformin treats type 2 diabetes and may cause nausea or diarrhea. Anything else I can help with?

