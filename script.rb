FILE_NAME = "debug_info"

lines = []

# Open the file and read it line by line, limiting to 337 lines
File.foreach(FILE_NAME).with_index do |line, index|
  break if index >= 337 # Stop after reading 337 lines
  lines << line.chomp  # Strip newline characters and add the line to the array
end

# Sort the lines alphabetically
sorted_lines = lines.sort

# Write the sorted lines back to the original file
File.open(FILE_NAME, "w") do |file|
  sorted_lines.each do |sorted_line|
    file.puts(sorted_line) # Write each sorted line to the file
  end
end

=begin
ziWkz4+r9CP4QT~=*O61mVBicuLWrZnrBXw*Hv5CRtd4&a3&=GeFg6Z@kOEdUr^QtxYWqIqnk=oUNk8e+e^U6ODJP1RHy5dlDp@H
~1YCQ@=z+SfL_=fsAzZcv#JI9rLhyVN~tazbYuC3oi8QmpRFa5KuXefHyE4gECU#iOwwME1l8J~Y+BkI9z#Qk+-6_y*dbh_YksQ0
~ykRZ9_I3_2mvl&yS8grf#r8hvf_vrUw9RlhA4EgTkYisn+xg#Ii=FYu8fNWuRkYCJGp4SF8_2MZnLkolBMOPog~SPk_Q2v*Os7*
zw39a#N_HLIWM3smR=FeW_8X&RH3ro-@g2I9C=bbIxtpL8h&b7C#7+Df_0LWhO73XWtVruVgTWF9lx~y_FqPW-KWkFk78KtrfT8T
z4^35QfB-Nn79LnO0M5tpsgo6F@mBQe@#Ba^zgmZ2nq3-7XMpPg7BA0*^IJ&B-4VA@##1Sb5wz0yWf47RCIy=l9aOq#90q4AgT7W
yFYwQ1vAhqvVixC+~y-0yQi+ne3sltbBWGWy=&5^AU22GnRdQ0A&WW*4Fpw7R*Pdm63&hNfMZ*+qy0g^bg_&ZA7OezkQ0mPzQlol
z+I~+wZ6^=@8u4Gk^2~tE^+dv2Lx5&ucWy3PiZhVl84pDZ9ganLwUHM059UN2ehqy_*DUFcZKhtQ^79ht-xBFiSHb=Wy3zXVyg3v
~R_wsH~yk#HT79wwoQR*^IzPEW#P-sB^m9^t3UZ47dW0i5aizRZlFNU=nY=GKCy0IFIh0e0aAtU1Lu0EkImDo2xM9~7DcHNDA@xR
~-ZA#eF^h76b+RrP_NDYDxp3*aBuwmM@*qelBs4+g-bvYh&+B=Gl4l+tR13FRecv7ghFz^+vMmlc#3AU&06Nm+__ph1F@lCcWWgF
~&P17@z0xgneiupBLL0#NvQmsH~&&52OJc=6vK3W^aNDLVIh3dSa+z6VWU65QL^&e9^7x08dg^Z@66KJ7M=mP^@SPmYJn~u95rr7
~hIMgd-_Tt=sVdSgUqEP4W@7dMgYqd9vFJLF4Z_aEF9Q0fb3P_hiBw87F6Fx5SrcnFuG2U=q3GbFQZN-BgGfSNG+=WGFD1Km&v-2
yw_iO0Uzxcngrqg_IcNOsKfHXT8oFpmOzI1p*+S&a6g0f7qSFM1=nl9e8e@vTHHD~voMr^F*odcmTm=Sk1qrq5ULV7XFx-E&#NaQ
~Ua5#2#o&641ztnx2yQTR~e0bOyknVQ1zhokO8GqFu&y^N4voPvWXu=lT8gxyZFYc&7upSQ=eaYWuqV33ecrECMwvJ4gnstxxw2S
ySvya8^yQtwKYToOcMFQZ&B~azgFcUlyVYMyiHbdMb@Po0tH+ykn4fbQMb=Q6AYsxixTFsFm#0Mo-*&rFBEwHQ*Vmm8H*rK=BWX4
=end
